import pytest
from unittest.mock import MagicMock
from datetime import datetime
import redis
from utils import (
    is_valid_filename,
    extract_date_from_filename,
    is_processed,
    mark_file_as_processed,
    calculate_file_hash,
)
import queue
import tempfile
import os

# Test `is_valid_filename`
@pytest.mark.parametrize("filename, expected", [
    ("btcusd-2023-10-01.csv", True),   # Valid filename
    ("ethusd-2023-10-01.csv", False),  # Wrong prefix
    ("btcusd-2023/10/01.csv", False),  # Wrong date format
    ("btcusd-2023-10-01", False),      # Missing `.csv`
    ("btcusd-20231001.csv", False),    # No hyphens in date
    ("", False),                       # Empty filename
    ("btcusd-2023-10-32.csv", False),  # Invalid day (32nd)
])
def test_is_valid_filename(filename, expected):
    """Test various filename validation cases."""
    assert is_valid_filename(filename) == expected, f"Failed for filename: {filename}"

# Test `extract_date_from_filename`
@pytest.mark.parametrize("filename, expected_date", [
    ("btcusd-2023-10-01.csv", datetime(2023, 10, 1)),  # ✅ Valid case
])
def test_extract_date_from_filename(filename, expected_date):
    """Test date extraction from valid filename formats."""
    assert extract_date_from_filename(filename) == expected_date, f"Failed to extract correct date from {filename}"

# Test `extract_date_from_filename` error handling
@pytest.mark.parametrize("invalid_filename", [
    "btcusd-2023/10/01.csv",  # Wrong date format
    "btcusd-2023-10-32.csv",  # Invalid day (32nd)
    "btcusd-20231001.csv",    # No hyphens in date
    "",                       # Empty filename
])
def test_extract_date_from_filename_invalid(invalid_filename):
    """Ensure extract_date_from_filename raises ValueError for invalid filenames."""
    with pytest.raises(ValueError):
        extract_date_from_filename(invalid_filename)

# Test `calculate_file_hash`
def test_calculate_file_hash():
    import hashlib
    content = b"test data content for hashing"
    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as temp:
        temp.write(content)
        temp_path = temp.name
    try:
        expected_hash = hashlib.sha256(content).hexdigest()
        assert calculate_file_hash(temp_path) == expected_hash
    finally:
        os.remove(temp_path)


# Test `is_processed`
def test_is_processed():
    mock_redis = MagicMock()
    mock_redis.sismember.return_value = True
    assert is_processed(mock_redis, 'dummy_hash') == True
    mock_redis.sismember.assert_called_once_with('processed_file_hashes', 'dummy_hash')

# Test `is_processed` with offline cache
def test_is_processed_offline_cache():
    offline_cache = {'hash_in_cache'}
    # Even if redis is down/None, checking offline cache should return True
    assert is_processed(None, 'hash_in_cache', offline_cache) == True
    assert is_processed(None, 'hash_not_in_cache', offline_cache) == False

# Test `is_processed` with Redis connection error
def test_is_processed_redis_error():
    mock_redis = MagicMock()
    mock_redis.sismember.side_effect = redis.exceptions.ConnectionError("Redis is down")
    assert is_processed(mock_redis, 'dummy_hash') == False

# Test `mark_file_as_processed`
def test_mark_file_as_processed():
    mock_redis = MagicMock()
    mark_file_as_processed(mock_redis, 'dummy_hash')
    mock_redis.sadd.assert_called_once_with('processed_file_hashes', 'dummy_hash')

# Test `mark_file_as_processed` with Redis connection error
def test_mark_file_as_processed_redis_error():
    mock_redis = MagicMock()
    mock_redis.sadd.side_effect = redis.exceptions.ConnectionError("Redis is down")
    
    retry_q = queue.Queue()
    offline_c = set()
    
    mark_file_as_processed(mock_redis, 'dummy_hash', retry_q, offline_c)
    mock_redis.sadd.assert_called_once_with('processed_file_hashes', 'dummy_hash')
    
    # Assert that it got buffered offline since Redis was down
    assert 'dummy_hash' in offline_c
    assert retry_q.get_nowait() == 'dummy_hash'