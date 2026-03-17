# AGENTS.md — duckdb-httpsql 开发指引

本文件供 AI 助手阅读。记录 `duckdb-httpsql` 扩展与配套 `httpsql-server` Go 服务的架构、构建方式和关键实现细节。

## 目录结构

```
/home/tao/workspace/db/duckdb/
├── duckdb-httpsql/           # DuckDB C++ 扩展 (git@github.com:vimt/duckdb-httpsql.git)
│   ├── duckdb/               # 子模块 — DuckDB v1.5.0
│   ├── extension-ci-tools/   # 子模块 — v1.5.0 分支
│   ├── src/
│   │   ├── include/
│   │   │   ├── httpsql_scanner.hpp       # HttpSQLBindData (继承 ArrowScanFunctionData)
│   │   │   ├── httpsql_ipc_stream.hpp    # HttpSQLMakeIPCStream()
│   │   │   ├── httpsql_catalog.hpp       # HttpSQLCatalog (HTTP keep-alive 连接池)
│   │   │   ├── httpsql_schema_entry.hpp  # Schema 缓存
│   │   │   ├── httpsql_table_entry.hpp   # GetScanFunction → 合成 ArrowSchema
│   │   │   └── httpsql_optimizer.hpp     # 聚合/Limit/Order pushdown
│   │   ├── httpsql_scanner.cpp           # HttpSQLProduce + ArrowTableFunction 集成
│   │   ├── httpsql_ipc_stream.cpp        # nanoarrow_ipc 解析器 → ArrowArrayStreamWrapper
│   │   ├── httpsql_optimizer.cpp         # 自定义优化器：agg/limit/order pushdown
│   │   ├── httpsql_http_client.cpp       # HTTP keep-alive 客户端
│   │   └── CMakeLists.txt
│   ├── CMakeLists.txt         # FetchContent nanoarrow (IPC) + link targets
│   └── .github/workflows/
│       ├── MainDistributionPipeline.yml  # 自定义 CI：Linux + macOS 并行，无 Windows
│       └── Tests.yml
└── httpsql-server/           # Go HTTP 服务 (独立仓库)
    ├── main.go
    ├── server/               # HTTP server，调用 backend.Backend 接口
    ├── backend/              # interface Backend { Schemas, Execute }
    ├── sharding/             # MySQL 分库分表实现 (implements Backend)
    │   ├── executor.go       # Arrow IPC 序列化 (github.com/apache/arrow/go/v18)
    │   └── sharding.go
    └── test/
        └── run_tests.py      # 集成测试脚本
```

---

## 架构概述

```
DuckDB query
    │
    ▼ optimizer callbacks (agg/limit/order pushdown → HttpSQLBindData)
    │
    ▼ ArrowScanInitGlobal → ProduceArrowScan → HttpSQLProduce()
    │   • 读取 HttpSQLBindData 中的 pushdown 状态
    │   • 构建 JSON 请求体
    │   • POST /api/query → httpsql-server
    │
    ▼ httpsql-server 返回 Arrow IPC stream
    │
    ▼ HttpSQLMakeIPCStream() → ArrowArrayStreamWrapper
    │   • nanoarrow_ipc 解析 Schema message + RecordBatch messages
    │
    ▼ ArrowTableFunction::ArrowScanFunction
        • DuckDB 原生 Arrow → DataChunk 转换 (ArrowToDuckDBConversion)
        • 正确处理 HUGEINT、所有类型
```

---

## 关键设计决策

### 1. 用 ArrowTableFunction 而不是自定义 Scan

**之前**：自定义 `HttpSQLScan` + 自定义 FlatBuffer parser (`httpsql_arrow_reader.cpp`)，存在以下 bug：
- `SUM(int)` 返回乱码（DOUBLE→HUGEINT 类型转换错误）
- `LIMIT + COUNT(*)` 触发 DuckDB 内部断言错误

**现在**：继承 `ArrowScanFunctionData`，使用 `ArrowTableFunction::ArrowScanFunction`：
- DuckDB 原生 Arrow 类型转换，所有 Arrow 类型都正确处理
- 不再需要自定义 FlatBuffer 解析

### 2. HttpSQLBindData 是 ArrowScanFunctionData

```cpp
struct HttpSQLBindData : public ArrowScanFunctionData {
    explicit HttpSQLBindData(HttpSQLTableEntry &table)
        : ArrowScanFunctionData(HttpSQLProduce,
                                reinterpret_cast<uintptr_t>(this))
        , table(table) {}

    HttpSQLTableEntry &table;
    vector<string> all_names;     // 所有列名（用于 WHERE 子句构建）
    string limit_clause;           // optimizer 写入
    string order_clause;           // optimizer 写入
    unique_ptr<AggPushdown> agg_pushdown;  // optimizer 写入
};
```

- `stream_factory_ptr = this`（堆分配，`make_uniq` 保证地址稳定）
- `HttpSQLProduce` 是 `stream_factory_produce_t` 回调，在 `ArrowScanInitGlobal` 时调用

### 3. HttpSQLProduce 的调用时机

```
Bind()               → 合成 ArrowSchema（从 catalog 列信息）
                       → 填充 ArrowScanFunctionData::schema_root + arrow_table
                       ← 不发 HTTP 请求

Optimizer callbacks  → 设置 agg_pushdown / limit_clause / order_clause

ArrowScanInitGlobal  → ProduceArrowScan()
                       → HttpSQLProduce()   ← 此时发 HTTP 请求
                         • parameters.projected_columns  投影列名
                         • parameters.filters            过滤器（DuckDB 自动填充）
                         → POST /api/query → Arrow IPC stream
```

### 4. nanoarrow_ipc 集成

- CMake `FetchContent` 下载 nanoarrow（commit `4bf5a93...`）
- `NANOARROW_IPC=ON`：编译 IPC 解析器
- `NANOARROW_NAMESPACE=DuckDBExtHttpsql`：C 符号重命名（`ArrowIpcDecoderInit` → `DuckDBExtHttpsql_ArrowIpcDecoderInit`），避免与 DuckDB 内置 `duckdb_nanoarrow` 冲突
- `httpsql_ipc_stream.cpp` 实现 `ArrowArrayStream` 的四个 C 回调（`get_schema` / `get_next` / `get_last_error` / `release`）

### 5. WHERE 子句构建

`HttpSQLProduce` 从 `ArrowStreamParameters` 读取 filters：

```cpp
for (auto &[filter_col_idx, filter] : parameters.filters->filters) {
    auto it = parameters.projected_columns.filter_to_col.find(filter_col_idx);
    // → phys_col → bind.all_names[phys_col] → col_name
}
```

`filter_to_col`：`ProduceArrowScan` 在 `ArrowScanInitGlobal` 中填充，将 filter 列索引映射到物理 schema 列索引。

---

## 构建

### 本地开发

```bash
cd duckdb-httpsql
# 全量构建（第一次）
USE_MERGED_VCPKG_MANIFEST=1 GEN=ninja make release

# 增量编译（只编 loadable extension）
cmake --build build/release --target httpsql_loadable_extension

# 运行（允许未签名扩展）
./build/release/duckdb -unsigned
```

### CMakeLists.txt 要点

**根 `CMakeLists.txt`**：
```cmake
include(FetchContent)
set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DuckDBExtHttpsql")
FetchContent_Declare(nanoarrow URL "...")
FetchContent_MakeAvailable(nanoarrow)

add_subdirectory(src)
build_static_extension(...)
build_loadable_extension(...)

# 使用 plain signature（与 extension_build_tools.cmake 一致）
target_link_libraries(httpsql_extension nanoarrow nanoarrow_ipc)
target_link_libraries(httpsql_loadable_extension nanoarrow nanoarrow_ipc)
```

**`src/CMakeLists.txt`**：
```cmake
add_library(httpsql_library OBJECT ...)
# 将 nanoarrow 的 include 路径传给 OBJECT 库
target_include_directories(httpsql_library PRIVATE
    $<TARGET_PROPERTY:nanoarrow_ipc,INTERFACE_INCLUDE_DIRECTORIES>)
```

---

## HTTP API（与 httpsql-server 通信）

### GET /api/schemas
返回所有 schema 名称列表（JSON）。

### GET /api/schemas/{schema}/tables
返回该 schema 下的表及列信息（JSON）。用于 catalog 构建和 bind 阶段的类型推断。

### POST /api/query
**请求体**（JSON）：
```json
{
  "schema": "my_schema",
  "table": "orders",
  "columns": ["id", "amount"],
  "where": "`status` = 'active'",
  "limit": " LIMIT 100",
  "order": " ORDER BY `id` ASC",
  "agg_select": "SUM(`amount`), COUNT(*)",
  "agg_group_by": "`status`"
}
```

**响应**：Arrow IPC stream（二进制），Schema message + RecordBatch messages。

---

## httpsql-server (Go)

```bash
cd httpsql-server
go build -o httpsql-server .
./httpsql-server --addr :8080 --dsn "root:password@tcp(host:3306)/db"
```

**backend 接口**：
```go
type Backend interface {
    Schemas() ([]string, error)
    Tables(schema string) ([]TableInfo, error)
    Execute(req QueryRequest) (io.Reader, error)
}
```

**Arrow IPC 序列化**（`sharding/executor.go`）：
- 使用 `github.com/apache/arrow/go/v18`
- `ipc.NewWriter(w, ipc.WithSchema(schema))` 写 Schema message
- 每批行写一个 RecordBatch（默认 1000 行/batch）
- MySQL 聚合结果通过 `rows.ColumnTypes()` 动态推断 Arrow 类型（COUNT→Int64, SUM→根据原列类型）

**集成测试**：
```bash
cd httpsql-server
# 需要先启动 httpsql-server（端口 8080）并构建好 extension
python3 test/run_tests.py
```

测试脚本路径：`test/run_tests.py`，extension 路径：`../duckdb-httpsql/build/release/extension/httpsql/httpsql.duckdb_extension`

---

## CI/CD

### duckdb-httpsql CI (GitHub Actions)

`.github/workflows/MainDistributionPipeline.yml`：自定义工作流（非 duckdb/extension-ci-tools reusable workflow）。

- **无 Windows 构建**
- `linux` job：`ubuntu-latest`，Docker 容器分别构建 `linux_amd64` 和 `linux_arm64`
- `macos` job：`macos-latest`，原生构建 `osx_arm64` 和 `osx_amd64`
- 两个 job 并行运行

---

## 已知问题与历史 Bug

| 问题 | 根因 | 状态 |
|------|------|------|
| `SUM(int)` 返回乱码 | 旧 FlatBuffer parser 不处理 DOUBLE→HUGEINT 转换 | ✅ 重构后由 DuckDB 原生处理 |
| `LIMIT + COUNT(*)` crash | 自定义 ArrowIPCReader 与 DuckDB DataChunk 交互异常 | ✅ 重构后消除 |
| CI Windows 构建超时 | upstream reusable workflow 串行 Linux→Windows | ✅ 改自定义并行工作流 |

---

## 重要文件索引

| 文件 | 作用 |
|------|------|
| `src/httpsql_scanner.cpp` | `HttpSQLProduce` + `CreateHttpSQLScanFunction` |
| `src/httpsql_ipc_stream.cpp` | nanoarrow_ipc 解析 + `HttpSQLMakeIPCStream` |
| `src/httpsql_table_entry.cpp` | `GetScanFunction`：合成 ArrowSchema + 填充 `ArrowScanFunctionData` |
| `src/httpsql_optimizer.cpp` | 聚合/Limit/TopN pushdown optimizer |
| `src/httpsql_catalog.cpp` | catalog 构建 + schema 缓存 |
| `src/httpsql_http_client.cpp` | HTTP keep-alive 连接池 |
| `CMakeLists.txt` | FetchContent nanoarrow + link targets |
| `src/CMakeLists.txt` | OBJECT 库 + nanoarrow include 路径 |
| `../httpsql-server/sharding/executor.go` | Arrow IPC 序列化（Go 端） |
| `../httpsql-server/test/run_tests.py` | 集成测试 |
