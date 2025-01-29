from datetime import datetime
import redis

# Validate filename pattern: btcusd-YYYY-MM-DD.csv
def is_valid_filename(filename):
    try:
        if (filename[0:7] != "btcusd-"):
            return False
        date_str = filename[7:17] # Extract date pattern from filename
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except (IndexError, ValueError):
        return False
    
# Assumes filename has valid pattern: btcusd-YYYY-MM-DD.csv
# Returns datetime format from filename substring
def extract_date_from_filename(filename):
    date_str = filename[7:17]
    return datetime.strptime(date_str, "%Y-%m-%d")

# Check if file has already been processed by checking Redis cache
def is_processed(redis_conn, filename):
    try:
        return redis_conn.sismember('processed_files', filename)
    except redis.exceptions.ConnectionError:
        print("Redis is down! Unable to check processed files.")
        return False  # Assume file has not been processed to avoid skipping new files

# Mark file as processed by adding filename to Redis cache
def mark_file_as_processed(redis_conn, filename):
    try:
        redis_conn.sadd('processed_files', filename)
    except redis.exceptions.ConnectionError:
        print("Redis is down! Unable to mark file as processed.")