import logging
import os
import time
from sqlalchemy import create_engine, text
from datetime import datetime
import pandas as pd
import redis
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import queue

from dotenv import load_dotenv
load_dotenv()

# Import Helper Functions
from utils import (
    is_valid_filename,
    extract_date_from_filename,
    is_processed,
    mark_file_as_processed,
    calculate_file_hash
)

# Load Environment Variable Configurations
DB_URI = os.getenv("DB_URI", "postgresql://postgres:password@localhost:5432/postgres")
# Location of BTC stock data
DATA_DIRECTORY = os.getenv("DATA_DIRECTORY", "./dataset-test")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
TABLE_NAME = os.getenv("TABLE_NAME", "bitcoin_stock_data")
MAX_THREADS = int(os.getenv("MAX_THREADS", 4))

# Global variables for Redis connectivity and buffering
redis_available = True
redis_lock = threading.Lock()
redis_retry_queue = queue.Queue()
offline_processed_cache = set()
offline_cache_lock = threading.Lock()

# Dynamically read ENABLE_MULTITHREADING from the environment
def is_multithreading_enabled():
    return os.getenv("ENABLE_MULTITHREADING", "False").lower() == "true"

# Initialize logger configuration
def setup_logger(debug=False):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger

logger = setup_logger(debug=os.getenv("DEBUG", "False").lower() == "true")

# Create database connection
engine = create_engine(DB_URI, pool_pre_ping=True, pool_size=10, max_overflow=20)

# Thread-local storage for Redis connections
thread_local = threading.local()

# Get a Redis connection for the current thread.
# Includes retry logic and checks global availability.
def get_redis_connection():
    global redis_available
    with redis_lock:
        if not redis_available:
            return None

    if not hasattr(thread_local, "redis_conn"):
        conn = None
        retries = 3
        delay = 1
        for attempt in range(1, retries + 1):
            try:
                conn = redis.StrictRedis(
                    host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True,
                    socket_connect_timeout=2, socket_timeout=2
                )
                conn.ping()
                thread_local.redis_conn = conn
                logger.info(f"Thread {threading.get_ident()} connected to Redis successfully on attempt {attempt}")
                with redis_lock:
                    redis_available = True
                break
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                logger.warning(f"Thread {threading.get_ident()} failed to connect to Redis on attempt {attempt}: {e}")
                if attempt < retries:
                    time.sleep(delay)
                    delay *= 2
        
        if not hasattr(thread_local, "redis_conn"):
            with redis_lock:
                redis_available = False
            logger.error(f"Thread {threading.get_ident()} failed to connect to Redis after {retries} attempts.")
            return None
    else:
        # Check if existing connection is still alive
        try:
            thread_local.redis_conn.ping()
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            logger.warning(f"Thread {threading.get_ident()}'s existing Redis connection went down.")
            if hasattr(thread_local, "redis_conn"):
                delattr(thread_local, "redis_conn")
            with redis_lock:
                redis_available = False
            return None

    return thread_local.redis_conn

# Background thread to check and reconnect to Redis, and flush the recovery queue
def redis_recovery_worker():
    global redis_available
    while True:
        try:
            time.sleep(5)
            # Only attempt reconnection if there are items in the queue, or if Redis is marked down
            if redis_retry_queue.empty() and redis_available:
                continue

            logger.debug("RedisRecoveryWorker: Checking/Attempting Redis connection...")
            conn = redis.StrictRedis(
                host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True,
                socket_connect_timeout=2, socket_timeout=2
            )
            conn.ping()

            # Successfully connected
            with redis_lock:
                redis_available = True

            flushed_count = 0
            while not redis_retry_queue.empty():
                try:
                    file_hash = redis_retry_queue.get_nowait()
                except queue.Empty:
                    break

                try:
                    conn.sadd('processed_file_hashes', file_hash)
                    flushed_count += 1
                    redis_retry_queue.task_done()
                except Exception as e:
                    redis_retry_queue.put(file_hash)
                    logger.error(f"RedisRecoveryWorker failed to write hash {file_hash} to Redis: {e}")
                    with redis_lock:
                        redis_available = False
                    break

            if flushed_count > 0:
                logger.info(f"RedisRecoveryWorker: Successfully flushed {flushed_count} processed hashes to Redis.")

        except Exception as e:
            with redis_lock:
                redis_available = False
            logger.debug(f"RedisRecoveryWorker: Redis is still unavailable: {e}")

# Helper to start the recovery worker thread
def start_redis_recovery_worker():
    worker = threading.Thread(target=redis_recovery_worker, daemon=True, name="RedisRecoveryWorker")
    worker.start()
    logger.info("Redis recovery worker thread started successfully.")


# Create table that will store bitcoin stock data
def initialize_database():
    with engine.begin() as db_conn:
        db_conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date_time TIMESTAMPTZ PRIMARY KEY,  -- Combined date and time
            open_price FLOAT,                  -- Open price
            high_price FLOAT,                  -- High price
            low_price FLOAT,                   -- Low price
            close_price FLOAT,                 -- Close price
            volume_btc FLOAT,                  -- Volume in BTC
            volume_currency FLOAT,             -- Volume in currency
            weighted_price FLOAT               -- Weighted price
        );
        """))
        create_hypertable(db_conn)
        
    logger.debug("Database initialized successfully")

# Create Timescale hypertable if it does not exist
def create_hypertable(db_conn):
    result = db_conn.execute(text(f"""
        SELECT *
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = '{TABLE_NAME}' AND n.nspname = 'public';
        """))
    # If hypertable doesn't exist, then create. Migrate data if data already exists in table.
    if result.fetchone() is None:
        print(f"Creating hypertable: {TABLE_NAME}")
        db_conn.execute(text(f"""
        SELECT create_hypertable('{TABLE_NAME}', 'date_time', if_not_exists => TRUE, migrate_data => TRUE);
        """))

# Process existing files in directory
def process_existing_files():
    files = [f for f in os.listdir(DATA_DIRECTORY) if f.endswith('.csv') and is_valid_filename(f)]
    files = sorted(files, key=extract_date_from_filename)  # Sort by extracted date

    if is_multithreading_enabled():
        logger.debug("Running in multithreaded mode.")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            for file in files:
                filepath = os.path.join(DATA_DIRECTORY, file)
                try:
                    file_hash = calculate_file_hash(filepath)
                except Exception as e:
                    logger.error(f"Failed to hash file {filepath}: {e}")
                    continue
                r = get_redis_connection()
                if not is_processed(r, file_hash, offline_processed_cache):  # Check if file has already been processed
                    futures.append(executor.submit(process_file, filepath, file_hash))
                else:
                    logger.debug(f"{filepath} (hash: {file_hash}) has already been processed")

            for future in as_completed(futures):
                try:
                    future.result()  # Wait for the task to complete and handle any exceptions
                except Exception as e:
                    logger.error(f"Error processing file: {e}")
    else:
        logger.debug("Running in non-multithreaded mode.")
        for file in files:
            filepath = os.path.join(DATA_DIRECTORY, file)
            try:
                file_hash = calculate_file_hash(filepath)
            except Exception as e:
                logger.error(f"Failed to hash file {filepath}: {e}")
                continue
            r = get_redis_connection()
            if not is_processed(r, file_hash, offline_processed_cache):
                process_file(filepath, file_hash)
            else:
                logger.debug(f"{filepath} (hash: {file_hash}) has already been processed")
    logger.debug("All files have been processed")

# Process single CSV file and store its data
def process_file(filepath, file_hash=None):
    thread_id = threading.get_ident()  # Get the current thread's identifier
    thread_name = threading.current_thread().name  # Get the current thread's name
    logger.info(f"Thread {thread_name} (ID: {thread_id}) is processing file: {filepath}.")
    try:
        if file_hash is None:
            file_hash = calculate_file_hash(filepath)
        process_file_data(filepath)
        r = get_redis_connection()
        with offline_cache_lock:
            mark_file_as_processed(r, file_hash, redis_retry_queue, offline_processed_cache)
        logger.info(f"Thread {thread_name} (ID: {thread_id}) finished processing file: {filepath}.")
    except Exception as e:
        logger.error(f"Thread {thread_name} (ID: {thread_id}) encountered an error processing file {filepath}: {e}")

# Perform ETL on CSV file data and store in DB
def process_file_data(filepath):
    try:
        df = pd.read_csv(filepath)
        filename = os.path.basename(filepath)
        file_date = extract_date_from_filename(filename)

        if 'Time' not in df.columns:
            raise ValueError("Missing 'Time' column in CSV file")

        # Drop rows where all relevant columns (except 'time') are empty
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume_(BTC)', 'Volume_(Currency)', 'Weighted_Price'], how='all')

        # Combine date from filename with time in CSV
        df['date_time'] = df['Time'].apply(lambda x: datetime.combine(file_date, datetime.strptime(x, "%H:%M:%S").time()))

        # Rename columns to match database schema
        df = df.rename(columns={
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume_(BTC)': 'volume_btc',
            'Volume_(Currency)': 'volume_currency',
            'Weighted_Price': 'weighted_price'
        })

        df_records = df[['date_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume_btc', 'volume_currency', 'weighted_price']]

        # Load dataframe records into database
        with engine.connect() as conn:
            df_records.to_sql(TABLE_NAME, conn, if_exists='append', index=False, method='multi')
    except Exception as e:
        raise RuntimeError(f"Failed to insert data from {filepath} into database: {e}")

# Observe directory to detect new files
def start_observer(directory):
    event_handler = NewFileHandler()
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()
    logger.info(f"Watching directory: {directory} for new files...")

    try:
        while True:
            time.sleep(1) # Keep script running
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Stopped watching directory")
    observer.join()

# File System Event Handler
class NewFileHandler(FileSystemEventHandler):
    def __init__(self):
        if is_multithreading_enabled():
            self.executor = ThreadPoolExecutor(max_workers=MAX_THREADS)  # Thread pool for processing files
        else:
            self.executor = None

    # Handles new files detected in directory
    def on_created(self, event):
        logger.debug(f"File event occurred: {event}")
        # ignore directory events
        if event.is_directory:
            return
        
        filepath = event.src_path

        try:
            filename = os.path.basename(filepath)
            if not is_valid_filename(filename):
                logger.info(f"Ignoring file with invalid filename pattern: {filename}")
                return

            file_hash = calculate_file_hash(filepath)
            r = get_redis_connection()
            if not is_processed(r, file_hash, offline_processed_cache):
                if is_multithreading_enabled():
                    # Submit file processing task to the thread pool
                    self.executor.submit(process_file, filepath, file_hash)
                else:
                    process_file(filepath, file_hash)
            else:
                logger.info(f"Skipping {filepath} that's already processed")
        except Exception as e:
            logger.error(f"Error processing file {filepath}: {e}")

if __name__ == "__main__":
    start_redis_recovery_worker()
    initialize_database()
    process_existing_files()
    start_observer(DATA_DIRECTORY)
