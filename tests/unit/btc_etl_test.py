import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
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
