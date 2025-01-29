# Description
An example Python ETL application that processes timescale data at scale from a folder directory & inserts it into a Timescale DB. Example data uses bitcoin price changes for every minute captured daily.

# Local Linux Build Setup
## Environment Variable Configuration
```export DB_URI=postgresql://postgres:password@localhost:5432/postgres
export DATA_DIRECTORY=./dataset-test
export REDIS_HOST=localhost
export REDIS_PORT=6379
export TABLE_NAME=bitcoin_stock_data
```

## Start Local Docker Timescale Postgres DB & Redis container
```
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:latest-pg17

docker run --name redis -p 6379:6379 -d redis
```

# Future Work
* Write unit/integration tests
* Support multi-threading for processing multiple files
    * Create a queue for ingesting files 
    * Focus on concurrency complexity for preventing threads from picking the same file & sharing a connection pool when interacting with Redis
    * Write performance/benchmarking tests
* Support temporary Redis failure
    * If Redis shutsdown, store processed file metadata in queue until Redis connection can be restored & continue processing new incoming data
* Support stronger data integrity checks
    * Instead of checking filename for uniqueness, store hash values of the data in Redis
