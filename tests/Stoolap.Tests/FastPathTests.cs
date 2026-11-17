// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using Stoolap;
using Stoolap.Ado;
using Stoolap.Native;
using Xunit;

namespace Stoolap.Tests;

/// <summary>
/// Covers the engine 0.5.0 one-call queries (stoolap_*_query_all), the
/// per-handle column header cache, and streaming reads of every value type.
/// </summary>
public class FastPathTests
{
    private static Database SeedMixed(int rows)
    {
        var db = Database.OpenInMemory();
        db.Execute(@"CREATE TABLE t (
            id INTEGER PRIMARY KEY, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP, j JSON, v VECTOR(2)
        )");
        using var tx = db.Begin();
        for (int i = 1; i <= rows; i++)
        {
            tx.Execute("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?, ?)",
                (long)i,
                i % 7 == 0 ? null : i + 0.5,
                $"name-{i}-ü",
                i % 2 == 0,
                DateTime.UnixEpoch.AddSeconds(i),
                $"{{\"i\":{i}}}",
                new float[] { i, -i });
        }
        tx.Commit();
        return db;
    }

    [Fact]
    public void QueryStream_ReadsEveryRowAndType()
    {
        const int n = 1000;
        using var db = SeedMixed(n);
        using var rows = db.QueryStream("SELECT id, f, s, b, ts, j, v FROM t ORDER BY id");

        int seen = 0;
        while (rows.Read())
        {
            seen++;
            Assert.Equal(seen, rows.GetInt64(0));
            if (seen % 7 == 0)
            {
                Assert.True(rows.IsDBNull(1));
                Assert.Null(rows.GetValue(1));
            }
            else
            {
                Assert.Equal(seen + 0.5, rows.GetDouble(1));
            }
            Assert.Equal($"name-{seen}-ü", rows.GetString(2));
            Assert.Equal(seen % 2 == 0, rows.GetBoolean(3));
            Assert.Equal(DateTime.UnixEpoch.AddSeconds(seen), rows.GetDateTime(4));
            Assert.Equal($"{{\"i\":{seen}}}", rows.GetString(5));
            Assert.Equal(new float[] { seen, -seen }, rows.GetVector(6));
            Assert.Equal(StoolapType.Blob, rows.GetFieldType(6));
            Assert.Equal(new float[] { seen, -seen }, (float[])rows.GetValue(6)!);
        }
        Assert.Equal(n, seen);
        Assert.False(rows.Read());
    }

    [Fact]
    public void QueryStream_EmptyResult_ReportsColumns()
    {
        using var db = SeedMixed(0);
        using var rows = db.QueryStream("SELECT id, s FROM t");
        Assert.Equal(new[] { "id", "s" }, rows.Columns);
        Assert.Equal(2, rows.ColumnCount);
        Assert.False(rows.Read());
        Assert.Throws<InvalidOperationException>(() => rows.GetInt64(0));
    }

    [Fact]
    public void DataReader_ReadsLargeResult()
    {
        const int n = 700;
        using var conn = new StoolapConnection($"Data Source=memory://fastpath-{Guid.NewGuid():N}");
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE r (id INTEGER PRIMARY KEY, s TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var tx = conn.BeginTransaction())
        {
            for (int i = 1; i <= n; i++)
            {
                using var ins = conn.CreateCommand();
                ins.Transaction = tx;
                ins.CommandText = "INSERT INTO r VALUES (@id, @s)";
                ins.Parameters.Add(new StoolapParameter("@id", (long)i));
                ins.Parameters.Add(new StoolapParameter("@s", "row" + i));
                ins.ExecuteNonQuery();
            }
            tx.Commit();
        }

        using var q = conn.CreateCommand();
        q.CommandText = "SELECT id, s FROM r ORDER BY id";
        using var reader = q.ExecuteReader();
        Assert.Equal(typeof(long), reader.GetFieldType(0));
        int seen = 0;
        while (reader.Read())
        {
            seen++;
            Assert.Equal(seen, reader.GetInt32(0));
            Assert.Equal("row" + seen, reader.GetString(1));
        }
        Assert.Equal(n, seen);
    }

    [Fact]
    public void PreparedQuery_PicksUpSchemaChange()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (a INTEGER)");
        db.Execute("INSERT INTO t VALUES (1)");
        using var stmt = db.Prepare("SELECT * FROM t");

        var before = stmt.Query();
        Assert.Equal(new[] { "a" }, before.Columns);
        Assert.Same(before.Columns, stmt.Query().Columns); // header cache hit

        db.Execute("ALTER TABLE t ADD COLUMN b TEXT");
        var after = stmt.Query();
        Assert.Equal(new[] { "a", "b" }, after.Columns);
        Assert.Equal(2, after[0].Length);
    }

    [Fact]
    public void Query_ColumnCacheDoesNotLeakAcrossShapes()
    {
        using var db = Database.OpenInMemory();
        Assert.Equal(new[] { "x" }, db.Query("SELECT 1 AS x").Columns);
        Assert.Equal(new[] { "y", "z" }, db.Query("SELECT 1 AS y, 2 AS z").Columns);
        Assert.Equal(new[] { "x" }, db.Query("SELECT 1 AS x").Columns);
    }

    [Fact]
    public void Transaction_Queries_SeeUncommittedWrites()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        using var stmt = db.Prepare("SELECT v FROM t WHERE id = ?");
        using var tx = db.Begin();
        tx.Execute("INSERT INTO t VALUES (1, 'in-tx')");

        Assert.Equal("in-tx", tx.Query("SELECT v FROM t WHERE id = 1")[0, 0]);
        Assert.Equal("in-tx", tx.Query("SELECT v FROM t WHERE id = ?", 1L)[0, 0]);
        Assert.Equal("in-tx", tx.Query(stmt, 1L)[0, 0]);
        Assert.Equal(0, db.Query("SELECT v FROM t WHERE id = 1").RowCount);
        tx.Rollback();
    }

    [Fact]
    public void Transaction_OutlivesDisposedDatabase()
    {
        var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        var tx = db.Begin();
        tx.Execute("INSERT INTO t VALUES (1)");
        db.Dispose();

        // The transaction holds a reference on the connection handle, so the
        // one-call query path still has a live handle to run on.
        Assert.Equal(1L, tx.Query("SELECT COUNT(*) FROM t")[0, 0]);
        tx.Commit();
    }

    [Fact]
    public void Query_Error_ReportsEngineMessage()
    {
        using var db = Database.OpenInMemory();
        var ex = Assert.Throws<StoolapException>(() => db.Query("SELECT * FROM missing_table"));
        Assert.Contains("missing_table", ex.Message);

        using var tx = db.Begin();
        var txEx = Assert.Throws<StoolapException>(() => tx.Query("SELECT * FROM missing_table WHERE 1 = ?", 1L));
        Assert.Contains("missing_table", txEx.Message);
    }

    [Fact]
    public void Query_MissingParameter_IsAnError()
    {
        using var db = Database.OpenInMemory();
        Assert.Throws<StoolapException>(() => db.Query("SELECT ? + ?", 1L));
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void LeakTransaction(string dsn)
    {
        var db = Database.Open(dsn);
        db.Execute("CREATE TABLE leaked (id INTEGER)");
        _ = db.Begin(); // never committed, rolled back or disposed
        db.Dispose();
    }

    [Fact]
    public void LeakedTransaction_ReleasesItsDatabaseWhenFinalized()
    {
        var dsn = $"memory://leak-{Guid.NewGuid():N}";
        LeakTransaction(dsn);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        GC.WaitForPendingFinalizers();

        // The last handle on a memory:// DSN closed, so its engine is gone and
        // a fresh open starts empty. Before the fix the leaked transaction
        // pinned the native handle and the table survived.
        using var db = Database.Open(dsn);
        Assert.Throws<StoolapException>(() => db.Query("SELECT * FROM leaked"));
    }
}
