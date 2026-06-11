import hashlib
from datetime import datetime
import redis

# Validate filename pattern: btcusd-YYYY-MM-DD.csv
def is_valid_filename(filename):
    try:
        if (filename[0:7] != "btcusd-"):
            return False
        if (len(filename) != 21):
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

# Calculate SHA-256 hash of file content in chunks
def calculate_file_hash(filepath):
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            hasher.update(chunk)
    return hasher.hexdigest()

# Check if file has already been processed by checking offline cache or Redis cache
def is_processed(redis_conn, file_hash, offline_cache=None):
    if offline_cache is not None:
        if file_hash in offline_cache:
            return True
            
    if redis_conn is None:
        return False
        
    try:
        return redis_conn.sismember('processed_file_hashes', file_hash)
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
        print("Redis is down! Unable to check processed file hashes.")
        return False

# Mark file as processed by adding hash to Redis cache or caching offline
def mark_file_as_processed(redis_conn, file_hash, retry_queue=None, offline_cache=None):
    success = False
    if redis_conn is not None:
        try:
            redis_conn.sadd('processed_file_hashes', file_hash)
            success = True
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            print("Redis is down! Unable to mark file as processed.")
            
    if not success:
        if offline_cache is not None:
            offline_cache.add(file_hash)
        if retry_queue is not None:
            retry_queue.put(file_hash)