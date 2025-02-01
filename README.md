# Description
An example Python ETL application that processes timescale data at scale from a folder directory & inserts it into a Timescale DB. Example data uses bitcoin price changes for every minute captured daily.

# Local Linux Build Setup
## Environment Variable Configuration
```export DB_URI=postgresql://postgres:password@localhost:5432/postgres
export DATA_DIRECTORY=./dataset-test
export REDIS_HOST=localhost
export REDIS_PORT=6379
export TABLE_NAME=bitcoin_stock_data
export ENABLE_MULTITHREADING=True
export MAX_THREADS=4
```

## Start Local Docker Timescale Postgres DB & Redis container
```
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:latest-pg17

docker run --name redis -p 6379:6379 -d redis
```

# Future Work
* Write unit/integration tests
    * Initial unit tests written 1.29.2025
* Support multi-threading for processing multiple files (Completed 1.31.2025)
    * Create a queue for ingesting files 
    * Focus on concurrency complexity for preventing threads from picking the same file & sharing a connection pool when interacting with Redis
    * Write performance/benchmarking tests
    * UPDATE 1.31.2025: benchmark tests with test data seems to indicate a 20% or higher performance improvement when using multi-threading with 4 threads
* Support temporary Redis failure
    * If Redis shutsdown, store processed file metadata in queue until Redis connection can be restored & continue processing new incoming data
    * Include retry logic for establishing connection to Redis
* Support stronger data integrity checks
    * Instead of checking filename for uniqueness, store hash values of the data in Redis
