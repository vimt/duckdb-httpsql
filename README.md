# duckdb-httpsql

A DuckDB extension that queries remote databases through an HTTP intermediary server,
decoupling the DuckDB query engine from the underlying data source.

## Repositories

| Repo | Description |
|------|-------------|
| [`vimt/duckdb-httpsql`](https://github.com/vimt/duckdb-httpsql) | C++ DuckDB extension (this repo) |
| [`vimt/httpsql-server`](https://github.com/vimt/httpsql-server) | Go HTTP server — schema discovery & query routing |

## Architecture

```
DuckDB (C++ extension)
    │
    │  HTTP/1.1 (NDJSON)
    ▼
httpsql-server (Go)          ← schema cache, query routing, backend interface
    │
    │  SQL (go-sql-driver / any backend)
    ▼
MySQL / sharded MySQL
```

The extension registers a `STORAGE` handler. When you `ATTACH` a URL, it fetches
schemas and tables from the Go server. Query execution, sharding logic, and schema
caching all live in the Go server — the C++ extension only handles catalog integration
and optimizer pushdown (filters, ORDER BY, LIMIT, aggregates).

The Go server is structured around a clean `backend.Backend` interface, making it easy
to add new data sources (Postgres, Trino, …) without touching the C++ extension.

## Building

### Prerequisites

- CMake ≥ 3.21, C++17 compiler (gcc / clang), ninja (optional)
- Go ≥ 1.21 (for the server — separate repo)

### DuckDB extension

```bash
git clone --recurse-submodules https://github.com/vimt/duckdb-httpsql.git
cd duckdb-httpsql

# First build (bootstraps vcpkg)
make release

# Incremental rebuild after code changes (~5–30 s)
cmake --build build/release --target httpsql_loadable_extension
```

### Go server (separate repo)

```bash
git clone https://github.com/vimt/httpsql-server.git
cd httpsql-server
go build -o httpsql-server .
```

## Running

Start the Go server pointing at one or more MySQL DSNs:

```bash
./httpsql-server \
  --host "root:pass@tcp(host1:3306)/" \
  --host "root:pass@tcp(host2:3306)/" \
  --cache /tmp/httpsql.cache.duckdb \
  --ttl 3600 \
  --port 8080
```

Load the extension in DuckDB and attach:

```sql
-- Allow unsigned extensions during development:
-- ./duckdb/build/release/duckdb -unsigned

LOAD 'build/release/extension/httpsql/httpsql.duckdb_extension';

ATTACH 'http://localhost:8080' AS db (TYPE httpsql);

SELECT * FROM db.my_schema.my_table LIMIT 10;
```

### Server flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | (required) | MySQL DSN, repeatable for multiple shards |
| `--cache` | (in-memory) | SQLite schema cache file path |
| `--ttl` | 3600 | Schema cache TTL in seconds (0 = no expiry) |
| `--port` | 8080 | HTTP listen port |

## Server HTTP API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/schemas` | List all logical schema names |
| `GET` | `/api/schemas/{schema}/tables` | List tables with column metadata |
| `POST` | `/api/query` | Execute query, stream NDJSON rows |
| `POST` | `/api/refresh` | Force schema refresh |
| `DELETE` | `/api/schemas/{schema}/cache` | Clear and refresh a schema's cache |

### Query request body (POST /api/query)

```json
{
  "schema": "my_db",
  "table": "orders",
  "columns": ["id", "amount"],
  "where": "`status` = 'paid'",
  "order": " ORDER BY id DESC",
  "limit": " LIMIT 100",
  "agg_select": "",
  "agg_group_by": ""
}
```

### NDJSON response format

```
{"columns":[{"name":"id","type":"BIGINT","nullable":false},{"name":"amount","type":"DOUBLE","nullable":true}]}
[1001, 99.9]
[1002, 149.0]
```

## Testing

```bash
# Start MySQL
docker run -d -e MYSQL_ROOT_PASSWORD=root -p 3306:3306 mysql:8.0

# Load test data
mysql -h 127.0.0.1 -u root -proot < test/setup_test_data.sql

# Build and start server (from httpsql-server repo)
./httpsql-server --host "root:root@tcp(127.0.0.1:3306)/" &

# Build extension
make release

# Run integration tests
python3 test/run_tests.py
```

## CI/CD

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `Tests.yml` | push / PR | Clones httpsql-server, builds both, runs integration tests against MySQL |
| `MainDistributionPipeline.yml` | push / PR | Builds loadable `.duckdb_extension` for all platforms |

## Submodules

```bash
git clone --recurse-submodules https://github.com/vimt/duckdb-httpsql.git
# or after clone:
git submodule update --init --recursive
```

| Submodule | Pinned version |
|-----------|---------------|
| `duckdb` | v1.5.0 |
| `extension-ci-tools` | v1.5.0 branch |
