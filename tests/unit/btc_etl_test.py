import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
import warnings
import threading
import redis

# Mock Redis globally before importing btc_etl
with patch("redis.StrictRedis") as mock_redis:
    mock_redis.return_value.ping.return_value = True  # Simulate successful Redis connection
    from btc_etl import initialize_database, create_hypertable, process_file_data, get_redis_connection, TABLE_NAME

# ---------------------------
# FIXTURES FOR REUSABILITY
# ---------------------------

@pytest.fixture
def mock_db_connection():
    """Fixture to mock SQLAlchemy engine connection."""
    mock_conn = MagicMock()
    with patch("btc_etl.engine.connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        yield mock_conn  # Return mock connection


@pytest.fixture
def sample_dataframe():
    """Fixture to return a sample DataFrame mimicking CSV input."""
    return pd.DataFrame({
        "Time": ["12:00:00"],
        "Open": [50000],
        "High": [51000],
        "Low": [49000],
        "Close": [50500],
        "Volume_(BTC)": [100],
        "Volume_(Currency)": [5000000],
        "Weighted_Price": [50250],
    })


# ---------------------------
# TEST DATABASE FUNCTIONS
# ---------------------------

# Test that initialize_database creates the necessary table and hypertable.
def test_initialize_database(mock_db_connection):    
    mock_db_connection.execute.return_value.fetchone.return_value = None  

    initialize_database()

    # Validate table and hypertable creation queries were executed
    executed_queries = [call[0][0].text for call in mock_db_connection.execute.call_args_list]
    
    assert any("CREATE TABLE IF NOT EXISTS" in query for query in executed_queries)
    assert any("SELECT create_hypertable" in query for query in executed_queries)

# Test that create_hypertable runs the correct SQL command.
def test_create_hypertable(mock_db_connection):
    
    mock_db_connection.execute.return_value.fetchone.return_value = None  

    create_hypertable(mock_db_connection)

    # Validate hypertable creation query was executed
    mock_db_connection.execute.assert_called()
    assert "SELECT create_hypertable" in mock_db_connection.execute.call_args[0][0].text


# ---------------------------
# TEST FILE PROCESSING
# ---------------------------

# Test that process_file_data correctly reads, processes, and inserts data into the database.
def test_process_file_data(mock_db_connection, sample_dataframe):
    # Mock pandas.read_csv and DataFrame.to_sql
    with patch("pandas.read_csv", return_value=sample_dataframe):
        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            
            # Suppress warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                
                process_file_data("btcusd-2023-10-01.csv")

            # Validate to_sql was called correctly
            mock_to_sql.assert_called_once_with(
                TABLE_NAME,
                mock_db_connection,
                if_exists="append",
                index=False,
                method="multi",
            )

# Test that process_file_data raises an error when 'Time' column is missing.
def test_process_file_data_missing_time_column(mock_db_connection):    
    # Sample DataFrame without 'Time' column
    df_missing_time = pd.DataFrame({
        "Open": [50000],
        "High": [51000],
        "Low": [49000],
        "Close": [50500],
        "Volume_(BTC)": [100],
        "Volume_(Currency)": [5000000],
        "Weighted_Price": [50250],
    })

    with patch("pandas.read_csv", return_value=df_missing_time):
        with pytest.raises(RuntimeError, match="Missing 'Time' column in CSV file"):
            process_file_data("btcusd-2023-10-01.csv")

    # Ensure no database insertions occurred
    mock_db_connection.execute.assert_not_called()

# ---------------------------
# TEST REDIS CONNECTION
# ---------------------------

# Test that get_redis_connection creates a new Redis connection for each thread.
def test_get_redis_connection():
    with patch("redis.StrictRedis") as mock_redis:
        mock_redis.return_value.ping.return_value = True  # Simulate successful Redis connection

        # Simulate two different threads
        def thread_function():
            redis_conn = get_redis_connection()
            assert redis_conn == mock_redis.return_value

        thread1 = threading.Thread(target=thread_function)
        thread2 = threading.Thread(target=thread_function)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        # Ensure Redis connection was created for each thread
        assert mock_redis.call_count == 2


# ---------------------------
# TEST RETRIES & OFFLINE QUEUE
# ---------------------------

def test_get_redis_connection_failure():
    import btc_etl
    # Reset global state
    with btc_etl.redis_lock:
        btc_etl.redis_available = True
    if hasattr(btc_etl.thread_local, "redis_conn"):
        delattr(btc_etl.thread_local, "redis_conn")
        
    with patch("redis.StrictRedis") as mock_redis:
        mock_redis.return_value.ping.side_effect = redis.exceptions.ConnectionError("Offline")
        # Mock sleep to make the test fast
        with patch("time.sleep") as mock_sleep:
            conn = btc_etl.get_redis_connection()
            assert conn is None
            # Check that StrictRedis was called 3 times (retries)
            assert mock_redis.call_count == 3
            # Check that it marked redis as unavailable
            assert btc_etl.redis_available is False


def test_redis_recovery_worker():
    import btc_etl
    # Reset global state
    with btc_etl.redis_lock:
        btc_etl.redis_available = False
    # Clear the queue
    while not btc_etl.redis_retry_queue.empty():
        try:
            btc_etl.redis_retry_queue.get_nowait()
        except Exception:
            break
            
    btc_etl.redis_retry_queue.put("test_hash_1")
    btc_etl.redis_retry_queue.put("test_hash_2")
    
    with patch("redis.StrictRedis") as mock_redis:
        mock_conn = MagicMock()
        mock_redis.return_value = mock_conn
        
        # Mock sleep to run once and exit by throwing a BaseException
        with patch("time.sleep", side_effect=[None, BaseException("exit worker")]):
            with pytest.raises(BaseException, match="exit worker"):
                btc_etl.redis_recovery_worker()
        
        # Verify connection succeeded, redis_available set to True
        assert btc_etl.redis_available is True
        # Verify elements were popped and sadd was called
        assert mock_conn.sadd.call_count == 2
        mock_conn.sadd.assert_any_call('processed_file_hashes', 'test_hash_1')
        mock_conn.sadd.assert_any_call('processed_file_hashes', 'test_hash_2')
        assert btc_etl.redis_retry_queue.empty()



# ---------------------------
# TEST SCANNER AND PROCESSING
# ---------------------------

def test_process_existing_files_single_threaded():
    import btc_etl
    with patch("btc_etl.is_multithreading_enabled", return_value=False):
        with patch("os.listdir", return_value=["btcusd-2023-10-01.csv", "btcusd-2023-10-02.csv"]):
            with patch("btc_etl.calculate_file_hash", side_effect=["hash1", "hash2"]):
                with patch("btc_etl.is_processed", side_effect=[False, True]):
                    with patch("btc_etl.get_redis_connection", return_value=MagicMock()):
                        with patch("btc_etl.process_file") as mock_process_file:
                            btc_etl.process_existing_files()
                            # Should process the first file, and skip the second
                            mock_process_file.assert_called_once_with(
                                os.path.join(btc_etl.DATA_DIRECTORY, "btcusd-2023-10-01.csv"), "hash1"
                            )


def test_process_file():
    import btc_etl
    with patch("btc_etl.process_file_data") as mock_process_data:
        with patch("btc_etl.get_redis_connection") as mock_get_redis:
            with patch("btc_etl.mark_file_as_processed") as mock_mark:
                mock_redis_conn = MagicMock()
                mock_get_redis.return_value = mock_redis_conn
                
                btc_etl.process_file("dummy_path", "dummy_hash")
                
                mock_process_data.assert_called_once_with("dummy_path")
                mock_mark.assert_called_once_with(
                    mock_redis_conn, "dummy_hash", btc_etl.redis_retry_queue, btc_etl.offline_processed_cache
                )


def test_new_file_handler_on_created():
    import btc_etl
    handler = btc_etl.NewFileHandler()
    mock_event = MagicMock()
    mock_event.is_directory = False
    mock_event.src_path = "dataset-test/btcusd-2023-10-01.csv"
    
    with patch("btc_etl.calculate_file_hash", return_value="hash123"):
        with patch("btc_etl.is_processed", return_value=False):
            with patch("btc_etl.get_redis_connection", return_value=MagicMock()):
                with patch("btc_etl.process_file") as mock_process_file:
                    handler.on_created(mock_event)
                    mock_process_file.assert_called_once_with("dataset-test/btcusd-2023-10-01.csv", "hash123")


def test_start_redis_recovery_worker():
    import btc_etl
    with patch("threading.Thread") as mock_thread:
        btc_etl.start_redis_recovery_worker()
        mock_thread.assert_called_once()
        mock_thread.return_value.start.assert_called_once()


def test_get_redis_connection_ping_fails():
    import btc_etl
    # Set up thread-local connection
    mock_conn = MagicMock()
    btc_etl.thread_local.redis_conn = mock_conn
    # Make ping fail
    mock_conn.ping.side_effect = redis.exceptions.ConnectionError("ping fail")
    
    with btc_etl.redis_lock:
        btc_etl.redis_available = True
        
    conn = btc_etl.get_redis_connection()
    assert conn is None
    assert btc_etl.redis_available is False
    assert not hasattr(btc_etl.thread_local, "redis_conn")


def test_redis_recovery_worker_sadd_fails():
    import btc_etl
    with btc_etl.redis_lock:
        btc_etl.redis_available = False
    # Clear the queue
    while not btc_etl.redis_retry_queue.empty():
        try:
            btc_etl.redis_retry_queue.get_nowait()
        except Exception:
            break
            
    btc_etl.redis_retry_queue.put("fail_hash")
    
    with patch("redis.StrictRedis") as mock_redis:
        mock_conn = MagicMock()
        mock_redis.return_value = mock_conn
        mock_conn.sadd.side_effect = Exception("sadd failed")
        
        with patch("time.sleep", side_effect=[None, BaseException("exit worker")]):
            with pytest.raises(BaseException, match="exit worker"):
                btc_etl.redis_recovery_worker()
                
        # Verify redis marked as unavailable again after sadd failed
        assert btc_etl.redis_available is False
        # Verify item is still in queue
        assert btc_etl.redis_retry_queue.qsize() == 1
        assert btc_etl.redis_retry_queue.get_nowait() == "fail_hash"


def test_process_existing_files_multithreaded():
    import btc_etl
    with patch("btc_etl.is_multithreading_enabled", return_value=True):
        with patch("os.listdir", return_value=["btcusd-2023-10-01.csv"]):
            with patch("btc_etl.calculate_file_hash", return_value="hash_multi"):
                with patch("btc_etl.is_processed", return_value=False):
                    with patch("btc_etl.get_redis_connection", return_value=MagicMock()):
                        with patch("btc_etl.ThreadPoolExecutor") as mock_executor:
                            with patch("btc_etl.as_completed", side_effect=lambda fs: fs):
                                btc_etl.process_existing_files()
                                mock_executor.return_value.__enter__.return_value.submit.assert_called_once()




