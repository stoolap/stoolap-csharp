# Stoolap for .NET

[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0-512BD4)](#)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

High-performance .NET driver and ADO.NET provider for [Stoolap](https://stoolap.io), an embedded SQL database with MVCC, columnar indexes, time-travel queries, and full ACID compliance.

Binds directly to `libstoolap` through source-generated `[LibraryImport]` P/Invoke. No C/C++ shim, no JNI, no per-call IL stub. The ADO.NET layer lets Stoolap slot into Dapper, LINQ to DB, or any framework that speaks `System.Data.Common`.

## Highlights

- **`[LibraryImport]` P/Invoke**. Compile-time source-generated stubs, AOT-compatible.
- **Zero-allocation parameter binding**. Hot path uses `stackalloc Span<StoolapValue>` and a 1 KiB stack scratch buffer for UTF-8 payloads. Driver allocates **0 bytes per call**.
- **Two read paths**. `Query()` runs the statement and packs every row in one P/Invoke call (`stoolap_query_params_all` / `stoolap_stmt_query_all`, engine 0.5.0), and `QueryStream()` exposes per-row accessors for large result sets.
- **Full ADO.NET provider**. `StoolapConnection`, `StoolapCommand`, `StoolapDataReader`, `StoolapTransaction`, `StoolapParameter`, `StoolapConnectionStringBuilder`.
- **Named parameter rewriting**. ADO.NET's `@name` / `:name` / `$name` placeholders are translated to positional `?` with full SQL-lexer awareness (literals, quoted identifiers, line/block comments).
- **`SafeHandle` for every opaque pointer**. Exception-safe resource cleanup.
- Full xUnit test suite and a comparison benchmark project versus Microsoft.Data.Sqlite.

## Benchmark

A comparison benchmark against Microsoft.Data.Sqlite lives in `benchmark/`. It runs a fixed set of operations against both drivers on the same in-memory dataset and prints per-operation timings plus an allocation probe on the write path.

```bash
dotnet run --project benchmark/Stoolap.Benchmark.csproj -c Release
```

## Repository Layout

```
stoolap-csharp/
├── Stoolap.sln
├── Directory.Build.props                 # shared MSBuild props
├── README.md
├── LICENSE
├── src/Stoolap/                          # main package
│   ├── Stoolap.csproj                    # net8.0;net9.0, IsAotCompatible
│   ├── Database.cs                       # high-level facade
│   ├── PreparedStatement.cs
│   ├── Transaction.cs
│   ├── Rows.cs                           # streaming row reader
│   ├── QueryResult.cs                    # materialized result
│   ├── ParameterBinder.cs                # zero-alloc parameter marshalling
│   ├── BinaryRowParser.cs                # decoder for the packed result buffer
│   ├── StoolapException.cs
│   ├── Native/
│   │   ├── NativeMethods.cs              # [LibraryImport] declarations
│   │   ├── StoolapValue.cs               # [StructLayout] FFI tagged union
│   │   ├── LibraryResolver.cs            # STOOLAP_LIB_PATH + RID lookup
│   │   ├── StatusCodes.cs                # status codes + type enums
│   │   └── Stoolap*Handle.cs             # SafeHandle subclasses
│   ├── Ado/                              # ADO.NET provider
│   │   ├── StoolapConnection.cs
│   │   ├── StoolapCommand.cs
│   │   ├── StoolapDataReader.cs
│   │   ├── StoolapTransaction.cs
│   │   ├── StoolapParameter.cs
│   │   ├── StoolapParameterCollection.cs
│   │   ├── StoolapConnectionStringBuilder.cs
│   │   └── NamedParameterRewriter.cs
│   └── build/Stoolap.targets             # native-binary copy MSBuild targets
├── tests/Stoolap.Tests/                  # xUnit suite (net8.0 + net9.0)
│   ├── SmokeTests.cs
│   ├── ParameterBinderTests.cs
│   ├── NamedParameterRewriterTests.cs
│   ├── ConnectionStringBuilderTests.cs
│   ├── CommandAndParameterTests.cs
│   ├── DataReaderTests.cs
│   ├── SqlFeatureTests.cs
│   ├── ErrorHandlingTests.cs
│   ├── TypeRoundTripTests.cs
│   ├── FastPathTests.cs
│   ├── RegressionTests.cs
│   ├── AdoContractTests.cs
│   └── AdoTests.cs
├── benchmark/                            # vs Microsoft.Data.Sqlite
│   ├── Stoolap.Benchmark.csproj
│   └── Program.cs
├── runtimes/                             # NuGet RID-specific natives
│   ├── osx-arm64/native/libstoolap.dylib
│   ├── osx-x64/native/libstoolap.dylib
│   ├── linux-x64/native/libstoolap.so
│   ├── linux-arm64/native/libstoolap.so
│   └── win-x64/native/libstoolap.dll
└── build/build-native.sh                 # cargo build --release + copy
```

## Requirements

- [.NET 8 SDK](https://dotnet.microsoft.com/download) or newer (tested on 8.0.419 and 9.0.312). The `global.json` pins the host SDK to 9.0 with `latestFeature` rollforward so the multi-target `net8.0;net9.0` build works from a single install.
- A `libstoolap` binary for your platform. Three options:
  1. Run `./build/build-native.sh`. The script auto-clones the stoolap engine at the pinned ref if neither `$STOOLAP_ROOT` nor a sibling `../stoolap/` checkout is available.
  2. Set `STOOLAP_LIB_PATH` to an existing `libstoolap.{dylib,so,dll}`.
  3. Drop a pre-built binary into `runtimes/<rid>/native/` yourself.

Supported RIDs: `osx-arm64`, `osx-x64`, `linux-x64`, `linux-arm64`, `win-x64`.

## Build & Test

```bash
# 1) Build the native binary for the host platform.
#    Auto-clones stoolap at $STOOLAP_ENGINE_REF (default v0.5.1)
#    if no source is found locally.
./build/build-native.sh

# 2) Build and test the managed assembly
dotnet build -c Release
dotnet test  -c Release

# 3) Run the comparison benchmark
dotnet run --project benchmark/Stoolap.Benchmark.csproj -c Release
```

Expected: all tests pass.

The script's stoolap-source resolution order:

1. `$STOOLAP_ROOT` if it points at a Cargo project.
2. `../stoolap` (sibling checkout next to this repo).
3. Auto-clone into `build/.stoolap-engine/` (gitignored, reused on subsequent runs).

```bash
# Explicit stoolap source location
STOOLAP_ROOT=/absolute/path/to/stoolap ./build/build-native.sh

# Different engine version (also picked up by the CI workflows)
STOOLAP_ENGINE_REF=v0.4.2 ./build/build-native.sh
```

## High-level API Quick Start

```csharp
using Stoolap;

using var db = Database.OpenInMemory();

db.Execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        age INTEGER
    )
""");

db.Execute("INSERT INTO users VALUES (?, ?, ?, ?)", 1, "Alice", "alice@example.com", 30);
db.Execute("INSERT INTO users VALUES (?, ?, ?, ?)", 2, "Bob",   "bob@example.com",   25);

var result = db.Query("SELECT id, name, age FROM users WHERE age > ?", 18);
foreach (var row in result.Rows)
{
    Console.WriteLine($"{row[0]} {row[1]} {row[2]}");
}
```

### Streaming Reader

For large result sets, prefer the streaming reader so rows are not all materialized at once:

```csharp
using var rows = db.QueryStream("SELECT id, name FROM users");
while (rows.Read())
{
    long id = rows.GetInt64(0);
    string? name = rows.GetString(1);
    Console.WriteLine($"{id} {name}");
}
```

### Prepared Statements

```csharp
using var insert = db.Prepare("INSERT INTO users VALUES (?, ?, ?, ?)");
for (int i = 0; i < 1000; i++)
{
    insert.Execute(i, $"user{i}", $"user{i}@example.com", 20 + (i % 60));
}

using var select = db.Prepare("SELECT name FROM users WHERE id = ?");
var r = select.Query(42);
Console.WriteLine(r[0, 0]);
```

### Transactions

```csharp
using var tx = db.Begin();
try
{
    tx.Execute("INSERT INTO users VALUES (?, ?, ?, ?)", 100, "Carol", "carol@example.com", 40);
    tx.Execute("UPDATE users SET age = age + 1 WHERE id = ?", 1);
    tx.Commit();
}
catch
{
    tx.Rollback();
    throw;
}
```

Disposing a transaction without committing automatically rolls it back.

Snapshot isolation:

```csharp
using Stoolap.Native; // StoolapIsolationLevel, StoolapType

using var tx = db.Begin(StoolapIsolationLevel.Snapshot);
var snapshot = tx.Query("SELECT * FROM users");
// ... other connections' writes are invisible here ...
tx.Commit();
```

### Threading

A single `Database` instance is thread-confined. For parallel workloads, call `Clone()` per worker. Clones share the engine (data, indexes, WAL) but each has its own executor and error state:

```csharp
using var main = Database.Open("file:///var/data/mydb");

Parallel.For(0, 8, _ =>
{
    using var local = main.Clone();
    var n = local.Query("SELECT COUNT(*) FROM t")[0, 0];
    Console.WriteLine(n);
});
```

Objects that come from a handle (`Transaction`, `PreparedStatement`, `Rows`, and the ADO.NET connection, command and reader) are not thread-safe either, but they are not tied to the thread that created them: using one from different threads one after another, for example across an `await`, is fine. A transaction that is never disposed is rolled back, and releases its connection, on the finalizer thread.

## ADO.NET / Dapper

```csharp
using Stoolap.Ado;
using Dapper;

await using var conn = new StoolapConnection("Data Source=memory://");
conn.Open();

await conn.ExecuteAsync("""
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)
""");

await conn.ExecuteAsync(
    "INSERT INTO users VALUES (@id, @name, @email)",
    new { id = 1, name = "Alice", email = "alice@example.com" });

var users = await conn.QueryAsync<User>(
    "SELECT id, name, email FROM users WHERE id >= @min",
    new { min = 1 });

foreach (var u in users)
{
    Console.WriteLine($"{u.Id} {u.Name}");
}

record User(long Id, string Name, string? Email);
```

### Reader and Connection Behaviour

Behaviour that ADO.NET consumers such as Dapper rely on:

- **Column types are learned from the data.** The engine reports each value's type, not a result column's declared type. The reader reads the first row when it opens, and when a column is NULL there it reads ahead (up to 64 rows, replayed in order) until every column has shown a value, so `GetFieldType` is right before the first `Read` even when the first rows hold NULLs. A column that is NULL in all of those rows, and every column of an empty result, reports `object`. Dapper copes with that when a class property's type is exactly the value's type (`long?`, `double?`, `string`, `DateTime?`, `bool?`): an empty result maps to an empty list, and so does `QueryFirstOrDefault`. Two cases still fail, because the engine does not report column types on its own: constructor mapping of a positional `record` on an empty result, and a narrower property (such as `int?` for an INTEGER column) whose column's first value comes after the first 64 rows. Prefer classes with exact property types for queries that can return no rows or long runs of NULLs.
- **Typed getters are strict.** `GetInt64`, `GetString` and the other typed getters throw `InvalidCastException` on a NULL value; check `IsDBNull` or read the value with `GetValue`, which returns `DBNull.Value`. A getter reads a value of its own type, and converts between INTEGER, FLOAT, BOOLEAN and numeric TEXT (`GetString` also formats numbers and booleans); any other conversion, such as `GetString` on a TIMESTAMP or `GetInt64` on non-numeric TEXT, throws `InvalidCastException`. An ordinal outside the result throws `IndexOutOfRangeException`.
- **`RecordsAffected`** is the number of rows a DML statement changed and -1 for a query, and stays readable after the reader is closed.
- **Closing the connection** closes its open readers and rolls back its transactions that have not ended. `CommandBehavior.CloseConnection` closes the connection when the reader closes.
- **`GetFieldValue<T>`** converts through the typed getters (`GetFieldValue<int>` works on an INTEGER column), and **`GetSchemaTable`** describes the result, so `DataTable.Load` works.
- **Not supported:** a command returns one result set (`NextResult` is always false, so Dapper's `QueryMultiple` cannot be used); `CommandTimeout` is not applied (set a per-connection limit with the `query_timeout` DSN key or `PRAGMA query_timeout`); a `Guid` is stored as TEXT, so Dapper needs a type handler (`SqlMapper.AddTypeHandler`) to map it back to a `Guid` property.

### Connection String Keywords

| Keyword | Description |
|---------|-------------|
| `Data Source` | DSN passed to `Database.Open` (e.g. `memory://`, `file:///path/to/db`) |
| `DataSource` | Alias for `Data Source` |
| `DSN` | Alias for `Data Source` |

### Named Parameter Rewriting

`StoolapCommand` rewrites named placeholders (`@name`, `:name`, `$name`) into numbered ones (`$1`, `$2`, ...), one number per distinct name, so a name used twice is one value. That matters on engine 0.5.0, which treats two anonymous `?` as two different expressions: `SELECT g + @x ... GROUP BY g + @x` only works when both refer to the same parameter. The rewrite leaves alone:

- Single-quoted string literals, including doubled-quote escapes (`'it''s @fine'`).
- Double-quoted and backtick-quoted identifiers, including doubled-quote escapes (`"col""@name"`, `` `a@b` ``).
- Line comments (`-- @ignored`) and block comments (`/* @ignored */`).
- The `::` cast operator (`x::INTEGER`).

A command whose SQL names a parameter that is not in its `Parameters` collection throws before it runs.

```csharp
using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM users WHERE email = @email AND age > @age";
cmd.Parameters.Add(new StoolapParameter("@email", "alice@example.com"));
cmd.Parameters.Add(new StoolapParameter("@age", 18));
using var reader = cmd.ExecuteReader();
```

The sigil (`@`, `:`, or `$`) is stripped from `ParameterName` during normalization, so `"@id"` and `"id"` refer to the same parameter in the collection.

## Type Mapping

| .NET (write) | Stoolap | .NET (read) |
|--------------|---------|-------------|
| `long`, `int`, `short`, `sbyte`, `byte`, `ushort`, `uint`, `ulong` | `INTEGER` | `long` |
| `double`, `float`, `decimal` | `FLOAT` | `double` |
| `string` | `TEXT` | `string` |
| `bool` | `BOOLEAN` | `bool` |
| `DateTime`, `DateTimeOffset` | `TIMESTAMP` (nanos UTC) | `DateTime` (UTC) |
| `string` (JSON) | `JSON` | `string` |
| `byte[]`, `ReadOnlyMemory<byte>` (packed little-endian f32, length a multiple of 4) | `VECTOR` | `float[]` |
| `float[]` | `VECTOR` | `float[]` |
| `Guid` | `TEXT` (string form) | `string` |
| `null`, `DBNull.Value` | `NULL` | `null` |
| `enum` | `INTEGER` (underlying value) | `long` |
| `char` | `TEXT` | `string` |

Any other parameter type throws `NotSupportedException` rather than being stored in some string form. `ulong` values above `long.MaxValue` throw `OverflowException`, and `DateTime` values outside 1677-09-21 to 2262-04-11 (the range of an i64 nanosecond `TIMESTAMP`) throw `ArgumentOutOfRangeException`.

Aggregate results (`SUM`, `AVG` over integer columns) may be returned as `long` or `double` depending on the planner's promotion rules. Use `Convert.ToInt64` / `Convert.ToDouble` on aggregate output.

## Performance Principles

1. **`[LibraryImport]`** source-generated marshalling. No per-call IL stubs, AOT-clean.
2. **UTF-8 end to end**. Stoolap is UTF-8 throughout, and `StringMarshalling.Utf8` skips the UTF-16 round trip a plain `DllImport` would force.
3. **`stackalloc Span<StoolapValue>`** for short parameter lists with **`stackalloc byte[1024]`** UTF-8 scratch, plus `[SkipLocalsInit]` so the scratch isn't zeroed on every call. Hot-path parameter binding allocates **zero bytes**.
4. **One-call queries** for materialized results (`Query()` on `Database`, `Transaction` and `PreparedStatement`): `stoolap_query_params_all` / `stoolap_stmt_query_all` run the statement and pack every row into a buffer owned by the handle. One P/Invoke call, no rows handle, nothing to free, then a decoder over a `ReadOnlySpan<byte>`.
5. **Cached column headers**. Each connection and prepared statement remembers the last result header; a repeated query with the same shape reuses the decoded column names instead of decoding them again. A schema change shows up as a different header and is decoded afresh.
6. **`SafeHandle`** for every opaque pointer so handles are released even on exception paths and AppDomain teardown.

## Architecture

```
+------------------------------------------------------+
|               Your .NET application                  |
+------------------------------------------------------+
|  Stoolap.Ado.*  (ADO.NET)  |  Stoolap.*  (core)      |
|  +-- StoolapConnection     |  +-- Database           |
|  +-- StoolapCommand        |  +-- PreparedStatement  |
|  +-- StoolapDataReader     |  +-- Transaction        |
|  +-- StoolapParameter      |  +-- Rows / QueryResult |
|  +-- NamedParameterRewriter|  +-- ParameterBinder    |
|  +-- StoolapTransaction    |  +-- BinaryRowParser    |
+------------------------------------------------------+
|  Stoolap.Native (bindings internal; public enums)     |
|  +-- NativeMethods  [LibraryImport] bindings          |
|  +-- StoolapValue   [StructLayout] tagged union       |
|  +-- SafeHandle wrappers                              |
|  +-- LibraryResolver (STOOLAP_LIB_PATH + RID lookup)  |
+------------------------------------------------------+
                          |
                          | P/Invoke (stable C ABI)
                          v
+------------------------------------------------------+
|    libstoolap.{dylib,so,dll}  (Rust, --features ffi) |
+------------------------------------------------------+
|              stoolap crate (Rust)                    |
|  MVCC, columnar indexes, volume storage, WAL         |
+------------------------------------------------------+
```

## Testing

xUnit suite across 13 files. Every test runs against both `net8.0` and `net9.0` target frameworks.

| File | Covers |
|---|---|
| `SmokeTests.cs` | open/close, execute, query, streaming, prepared, transactions, clone |
| `ParameterBinderTests.cs` | scratch-buffer fast path, HGlobal slow path, boundaries, all primitives |
| `NamedParameterRewriterTests.cs` | `@/:/$` sigils, literals, identifiers, comments, duplicates |
| `ConnectionStringBuilderTests.cs` | `DataSource` round-trip, alias normalization, indexer |
| `CommandAndParameterTests.cs` | command lifecycle, parameter collection, scalar/non-query, errors |
| `DataReaderTests.cs` | `FieldCount`, getters, `GetValues`, `IsDBNull`, `NextResult`, indexers |
| `SqlFeatureTests.cs` | aggregates, `GROUP BY`, `HAVING`, `ORDER BY`, joins, CTEs, subqueries |
| `ErrorHandlingTests.cs` | invalid SQL, disposed objects, transaction lifecycle, null args |
| `TypeRoundTripTests.cs` | every Stoolap type through binary and streaming paths |
| `FastPathTests.cs` | one-call queries, column header cache across schema changes, transaction lifetime, engine error messages |
| `AdoContractTests.cs` | Dapper mapping with NULL-first rows, type look-ahead, connection close and `CloseConnection`, `RecordsAffected`, ordinal/NULL errors, `GetFieldValue`, `GetSchemaTable`, parameter type rules |
| `AdoTests.cs` | ADO.NET connection lifecycle, reader streaming, transaction rollback |
| `RegressionTests.cs` | `HasRows` accuracy, `GetFieldType` schema stability before `Read()`, transaction-foreign-connection rejection, transactional `ExecuteReader` streaming |

```bash
dotnet test -c Release
```

## Error Handling

All driver errors surface as `StoolapException`:

```csharp
try
{
    db.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    db.Execute("INSERT INTO t VALUES (1)");
    db.Execute("INSERT INTO t VALUES (1)"); // duplicate PK
}
catch (StoolapException ex)
{
    Console.Error.WriteLine($"Database error ({ex.StatusCode}): {ex.Message}");
}
```

The same exception type flows through the ADO.NET layer, so code that catches `Exception` or `StoolapException` at the top of a request handler works uniformly for both APIs.

## Contributing

1. Fork the repo and create a topic branch.
2. Run `./build/build-native.sh` then `dotnet test -c Release` before committing.
3. New features land with tests: aim for the patterns already in `tests/Stoolap.Tests/*`.
4. The project is `TreatWarningsAsErrors=true`; please fix every warning before opening a PR.

## License

Apache-2.0. See [LICENSE](LICENSE).
