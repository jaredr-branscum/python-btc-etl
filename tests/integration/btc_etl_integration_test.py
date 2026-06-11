import os
import sys
import tempfile
import shutil
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, text
import pandas as pd
import redis
import queue
import time

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import btc_etl
from utils import calculate_file_hash, is_processed

@pytest.fixture
def temp_data_directory():
    """Create a temporary directory with mock CSV data files."""
    temp_dir = tempfile.mkdtemp()
    
    # Create file 1
    df1 = pd.DataFrame({
        "Time": ["00:01:00", "00:02:00"],
        "Open": [50000.0, 50100.0],
        "High": [50200.0, 50300.0],
        "Low": [49900.0, 50000.0],
        "Close": [50100.0, 50200.0],
        "Volume_(BTC)": [1.5, 2.0],
        "Volume_(Currency)": [75000.0, 100400.0],
        "Weighted_Price": [50050.0, 50180.0]
    })
    filepath1 = os.path.join(temp_dir, "btcusd-2023-10-01.csv")
    df1.to_csv(filepath1, index=False)
    
    # Create file 2
    df2 = pd.DataFrame({
        "Time": ["12:00:00"],
        "Open": [51000.0],
        "High": [51500.0],
        "Low": [50800.0],
        "Close": [51200.0],
        "Volume_(BTC)": [3.0],
        "Volume_(Currency)": [153600.0],
        "Weighted_Price": [51200.0]
    })
    filepath2 = os.path.join(temp_dir, "btcusd-2023-10-02.csv")
    df2.to_csv(filepath2, index=False)
    
    yield temp_dir
    
    # Cleanup
    shutil.rmtree(temp_dir)


def test_end_to_end_etl_pipeline(temp_data_directory):
    """Test the full ETL pipeline with in-memory SQLite and a simulated Redis server."""
    
    # Use SQLite in-memory for testing the DB interactions
    sqlite_engine = create_engine("sqlite:///:memory:")
    
    # Mock Redis class and client
    mock_redis_set = set()
    mock_redis_client = MagicMock()
    
    def mock_sismember(name, val):
        return val in mock_redis_set
        
    def mock_sadd(name, val):
        mock_redis_set.add(val)
        return 1
        
    mock_redis_client.sismember.side_effect = mock_sismember
    mock_redis_client.sadd.side_effect = mock_sadd
    mock_redis_client.ping.return_value = True

    # Patch modules in btc_etl
    with patch("btc_etl.engine", sqlite_engine), \
         patch("btc_etl.DATA_DIRECTORY", temp_data_directory), \
         patch("btc_etl.create_hypertable") as mock_hypertable, \
         patch("redis.StrictRedis", return_value=mock_redis_client), \
         patch("btc_etl.is_multithreading_enabled", return_value=False):
         
        # Reset btc_etl globals
        btc_etl.redis_available = True
        btc_etl.redis_retry_queue.queue.clear()
        btc_etl.offline_processed_cache.clear()
        if hasattr(btc_etl.thread_local, "redis_conn"):
            delattr(btc_etl.thread_local, "redis_conn")

        # 1. Initialize Database
        btc_etl.initialize_database()
        
        # Verify database table exists
        with sqlite_engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"), {"table_name": btc_etl.TABLE_NAME})
            row = result.fetchone()
            assert row is not None
            assert row[0] == btc_etl.TABLE_NAME

        # Calculate expected hashes
        hash1 = calculate_file_hash(os.path.join(temp_data_directory, "btcusd-2023-10-01.csv"))
        hash2 = calculate_file_hash(os.path.join(temp_data_directory, "btcusd-2023-10-02.csv"))

        # 2. Run ETL under healthy Redis conditions (File 1 only, mock listdir for File 1)
        with patch("os.listdir", return_value=["btcusd-2023-10-01.csv"]):
            btc_etl.process_existing_files()

        # Check DB has File 1 records
        with sqlite_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {btc_etl.TABLE_NAME}"))
            count = result.fetchone()[0]
            assert count == 2  # 2 rows in File 1

        # Check Redis mock has File 1 hash
        assert hash1 in mock_redis_set
        assert hash2 not in mock_redis_set

        # 3. Simulate Redis outage and run ETL for File 2
        mock_redis_client.ping.side_effect = redis.exceptions.ConnectionError("Redis connection lost")
        mock_redis_client.sadd.side_effect = redis.exceptions.ConnectionError("Redis connection lost")
        mock_redis_client.sismember.side_effect = redis.exceptions.ConnectionError("Redis connection lost")
        
        # Invalidate thread-local connection cache to trigger reconnect
        if hasattr(btc_etl.thread_local, "redis_conn"):
            delattr(btc_etl.thread_local, "redis_conn")
            
        with patch("os.listdir", return_value=["btcusd-2023-10-02.csv"]):
            # Mock sleep during connection attempt to keep test fast
            with patch("time.sleep") as mock_sleep:
                btc_etl.process_existing_files()

        # Check DB has File 2 records (inserted successfully despite Redis outage!)
        with sqlite_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {btc_etl.TABLE_NAME}"))
            count = result.fetchone()[0]
            assert count == 3  # 2 rows from File 1 + 1 row from File 2

        # Check that Redis mock does NOT have File 2 hash (since it failed)
        assert hash2 not in mock_redis_set
        
        # Check that File 2 hash is in the offline queue and local cache
        assert hash2 in btc_etl.offline_processed_cache
        assert btc_etl.redis_retry_queue.qsize() == 1
        assert btc_etl.redis_retry_queue.queue[0] == hash2

        # 4. Verify that running again does not double-process File 2
        # (It should check the local cache and skip processing)
        with patch("os.listdir", return_value=["btcusd-2023-10-02.csv"]):
            with patch("btc_etl.process_file") as mock_process:
                btc_etl.process_existing_files()
                mock_process.assert_not_called()

        # 5. Simulate Redis recovery and check background worker flushing queue
        mock_redis_client.ping.side_effect = None
        mock_redis_client.ping.return_value = True
        mock_redis_client.sadd.side_effect = mock_sadd
        mock_redis_client.sismember.side_effect = mock_sismember
        
        # Run recovery worker loop once
        with patch("time.sleep", side_effect=[None, BaseException("exit worker")]):
            with pytest.raises(BaseException, match="exit worker"):
                btc_etl.redis_recovery_worker()

        # Verify that Redis is marked as available
        assert btc_etl.redis_available is True
        
        # Verify that File 2 hash has been flushed and added to Redis
        assert hash2 in mock_redis_set
        assert btc_etl.redis_retry_queue.empty()
