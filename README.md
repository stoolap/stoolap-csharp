# Stoolap for .NET

[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0-512BD4)](#)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

High-performance .NET driver and ADO.NET provider for [Stoolap](https://stoolap.io), an embedded SQL database with MVCC, columnar indexes, time-travel queries, and full ACID compliance.

Binds directly to `libstoolap` through source-generated `[LibraryImport]` P/Invoke. No C/C++ shim, no JNI, no per-call IL stub. The ADO.NET layer lets Stoolap slot into Dapper, LINQ to DB, or any framework that speaks `System.Data.Common`.

## Highlights

- **`[LibraryImport]` P/Invoke**. Compile-time source-generated stubs, AOT-compatible.
- **Zero-allocation parameter binding**. Hot path uses `stackalloc Span<StoolapValue>` and a 1 KiB stack scratch buffer for UTF-8 payloads. Driver allocates **0 bytes per call**.
- **Two read paths**. `Database.Query()` uses the binary `stoolap_rows_fetch_all` buffer (one P/Invoke per call), and `Database.QueryStream()` exposes per-row accessors for large result sets.
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
│   ├── BinaryRowParser.cs                # decoder for stoolap_rows_fetch_all
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
├── tests/Stoolap.Tests/                  # 132 xUnit tests
│   ├── SmokeTests.cs
│   ├── ParameterBinderTests.cs
│   ├── NamedParameterRewriterTests.cs
│   ├── ConnectionStringBuilderTests.cs
│   ├── CommandAndParameterTests.cs
│   ├── DataReaderTests.cs
│   ├── SqlFeatureTests.cs
│   ├── ErrorHandlingTests.cs
│   ├── TypeRoundTripTests.cs
│   └── AdoTests.cs
├── benchmark/                            # vs Microsoft.Data.Sqlite
│   ├── Stoolap.Benchmark.csproj
│   └── Program.cs
├── runtimes/                             # NuGet RID-specific natives
│   ├── osx-arm64/native/libstoolap.dylib
│   ├── osx-x64/native/libstoolap.dylib
│   ├── linux-x64/native/libstoolap.so
│   ├── linux-arm64/native/libstoolap.so
│   └── win-x64/native/stoolap.dll
└── build/build-native.sh                 # cargo build --release + copy
```

## Requirements

- [.NET 8 SDK](https://dotnet.microsoft.com/download) or newer (tested on 8.0.419 and 9.0.312)
- A `libstoolap` binary for your platform. Either:
  1. Run `./build/build-native.sh` (requires a stoolap source checkout at `../stoolap` or the `STOOLAP_ROOT` env var),
  2. Set `STOOLAP_LIB_PATH` to an existing `libstoolap.{dylib,so,dll}`, or
  3. Drop a binary into `runtimes/<rid>/native/`.

Supported RIDs: `osx-arm64`, `osx-x64`, `linux-x64`, `linux-arm64`, `win-x64`.

## Build & Test

```bash
# 1) Build the native binary for the host platform
./build/build-native.sh

# 2) Build and test the managed assembly
dotnet build -c Release
dotnet test  -c Release

# 3) Run the comparison benchmark
dotnet run --project benchmark/Stoolap.Benchmark.csproj -c Release
```

Expected: all tests pass.

Override the stoolap source location:

```bash
STOOLAP_ROOT=/absolute/path/to/stoolap ./build/build-native.sh
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

### Connection String Keywords

| Keyword | Description |
|---------|-------------|
| `Data Source` | DSN passed to `Database.Open` (e.g. `memory://`, `file:///path/to/db`) |
| `DataSource` | Alias for `Data Source` |
| `DSN` | Alias for `Data Source` |

### Named Parameter Rewriting

Stoolap natively uses positional `?` placeholders. `StoolapCommand` rewrites named placeholders (`@name`, `:name`, `$name`) into positional `?`, preserving:

- Single-quoted string literals, including doubled-quote escapes (`'it''s @fine'`).
- Double-quoted identifiers, including doubled-quote escapes (`"col""@name"`).
- Line comments (`-- @ignored`) and block comments (`/* @ignored */`).

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
| `byte[]`, `ReadOnlyMemory<byte>` | `BLOB` | `byte[]` |
| `float[]` | `VECTOR` | `float[]` |
| `Guid` | `TEXT` (string form) | `string` |
| `null`, `DBNull.Value` | `NULL` | `null` |

Aggregate results (`SUM`, `AVG` over integer columns) may be returned as `long` or `double` depending on the planner's promotion rules. Use `Convert.ToInt64` / `Convert.ToDouble` on aggregate output.

## Performance Principles

1. **`[LibraryImport]`** source-generated marshalling. No per-call IL stubs, AOT-clean.
2. **UTF-8 end to end**. Stoolap is UTF-8 throughout, and `StringMarshalling.Utf8` skips the UTF-16 round trip a plain `DllImport` would force.
3. **`stackalloc Span<StoolapValue>`** for short parameter lists with **`stackalloc byte[1024]`** UTF-8 scratch, plus `[SkipLocalsInit]` so the scratch isn't zeroed on every call. Hot-path parameter binding allocates **zero bytes**.
4. **Binary fetch-all** for materialized queries (`Database.Query()`): one P/Invoke call per query, then a zero-copy decoder over a `ReadOnlySpan<byte>`.
5. **`SafeHandle`** for every opaque pointer so handles are released even on exception paths and AppDomain teardown.

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
|  Stoolap.Native (internal)                            |
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

132 xUnit tests across 10 files:

| File | Tests | Covers |
|---|---:|---|
| `SmokeTests.cs` | 11 | open/close, execute, query, streaming, prepared, transactions, clone |
| `ParameterBinderTests.cs` | 16 | scratch-buffer fast path, HGlobal slow path, boundaries, all primitives |
| `NamedParameterRewriterTests.cs` | 18 | `@/:/$` sigils, literals, identifiers, comments, duplicates |
| `ConnectionStringBuilderTests.cs` | 8 | `DataSource` round-trip, alias normalization, indexer |
| `CommandAndParameterTests.cs` | 18 | command lifecycle, parameter collection, scalar/non-query, errors |
| `DataReaderTests.cs` | 13 | `FieldCount`, getters, `GetValues`, `IsDBNull`, `NextResult`, indexers |
| `SqlFeatureTests.cs` | 17 | aggregates, `GROUP BY`, `HAVING`, `ORDER BY`, joins, CTEs, subqueries |
| `ErrorHandlingTests.cs` | 14 | invalid SQL, disposed objects, transaction lifecycle, null args |
| `TypeRoundTripTests.cs` | 16 | every Stoolap type through binary and streaming paths |
| `AdoTests.cs` | 3 | ADO.NET connection, reader, transaction rollback |

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
