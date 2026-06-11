# Description
An example Python ETL application that processes timescale data at scale from a folder directory & inserts it into a Timescale DB. Example data uses bitcoin price changes for every minute captured daily.

# Local Build Setup
## Environment Variable Configuration
The application uses a `.env` file to manage configuration options. Copy the template:
```bash
cp .env.example .env
```

And adjust any environment variables in `.env` as needed:
- `DB_URI`: Timescale Postgres database URI
- `DATA_DIRECTORY`: Directory containing BTC stock data CSV files
- `REDIS_HOST`: Redis hostname
- `REDIS_PORT`: Redis port
- `TABLE_NAME`: Database table name
- `ENABLE_MULTITHREADING`: Enable multithreading for file processing
- `MAX_THREADS`: Max threads for execution

## Start Local Environment using Docker Compose
Alternatively, stand up both Redis and TimescaleDB containers in the background using Docker Compose:
```bash
docker compose up -d
```

## Start Local Docker Timescale Postgres DB & Redis container (Legacy)
```
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:latest-pg17

docker run --name redis -p 6379:6379 -d redis
```

# Running Benchmarks
To compare the performance of single-threaded vs. multi-threaded file processing:
1. Ensure the local TimescaleDB and Redis containers are running (via Docker Compose).
2. Configure the required environment variables (`DB_URI`, `REDIS_HOST`, etc.).
3. Execute the benchmark script:
   ```bash
   python tests/benchmark.py
   ```
   *Note: This script will truncate the configured database table and clear the Redis processed cache before runs.*

# Future Work (Completed)
* **Write unit/integration tests** (Completed 06.11.2026)
    * Comprehensive pytest suite covering utils and ETL flows (>90% unit test coverage).
    * Integrated end-to-end integration test (`tests/integration/btc_etl_integration_test.py`) running database schema creation, CSV parsing, insertion (SQLite in-memory), Redis outage simulation, queue buffering, and connection recovery.
* **Support multi-threading for processing multiple files** (Completed 1.31.2025)
    * Focus on concurrency complexity for preventing threads from picking the same file & sharing a connection pool when interacting with Redis.
    * Benchmark tests with test data indicate a 40% or higher performance improvement when using multi-threading with 4 threads.
* **Support temporary Redis failure** (Completed 06.11.2026)
    * Built a connection retry mechanism with exponential backoff on startup/connect attempts.
    * Added thread-safe offline cache (`offline_processed_cache`) and recovery queue (`redis_retry_queue`). If Redis goes down, processing continues, storing processed data hashes offline.
    * Implemented a background recovery worker thread (`RedisRecoveryWorker`) that polls Redis and flushes the queue once connection is restored.
* **Support stronger data integrity checks** (Completed 06.11.2026)
    * Replaced filename uniqueness checks with SHA-256 binary data hashing (`calculate_file_hash` in `utils.py`). File hashes are stored and verified in Redis under the `processed_file_hashes` set.

