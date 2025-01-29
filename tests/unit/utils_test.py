import pytest
from datetime import datetime
from utils import is_valid_filename, extract_date_from_filename

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
