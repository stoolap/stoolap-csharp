// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using Stoolap;
using Xunit;

namespace Stoolap.Tests;

public class SmokeTests
{
    [Fact]
    public void Version_IsNotEmpty()
    {
        Assert.False(string.IsNullOrWhiteSpace(Database.Version));
    }

    [Fact]
    public void OpenInMemory_AndClose()
    {
        using var db = Database.OpenInMemory();
        Assert.NotNull(db);
    }

    [Fact]
    public void Execute_CreatesTable_AndInsertsRows()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        long inserted = db.Execute("INSERT INTO users VALUES (?, ?, ?)", 1, "alice", 30);
        Assert.Equal(1, inserted);

        var result = db.Query("SELECT id, name, age FROM users WHERE id = ?", 1);
        Assert.Equal(1, result.RowCount);
        Assert.Equal(3, result.ColumnCount);
        Assert.Equal(1L, result[0, 0]);
        Assert.Equal("alice", result[0, 1]);
        Assert.Equal(30L, result[0, 2]);
    }

    [Fact]
    public void Query_AllSupportedTypes()
    {
        using var db = Database.OpenInMemory();
        db.Execute(@"CREATE TABLE t (
            i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP, j JSON
        )");
        db.Execute("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)",
            42L, 3.14, "hello", true, new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc), "{\"k\":1}");

        var r = db.Query("SELECT * FROM t");
        Assert.Equal(1, r.RowCount);
        Assert.Equal(42L, r[0, 0]);
        Assert.Equal(3.14, (double)r[0, 1]!, 6);
        Assert.Equal("hello", r[0, 2]);
        Assert.Equal(true, r[0, 3]);
        Assert.IsType<DateTime>(r[0, 4]);
        Assert.Equal("{\"k\":1}", r[0, 5]);
    }

    [Fact]
    public void Streaming_Read_PerCellAccess()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE n (i INTEGER)");
        for (int i = 0; i < 10; i++)
        {
            db.Execute("INSERT INTO n VALUES (?)", i);
        }

        using var rows = db.QueryStream("SELECT i FROM n ORDER BY i");
        var collected = new List<long>();
        while (rows.Read())
        {
            collected.Add(rows.GetInt64(0));
        }
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), collected);
    }

    [Fact]
    public void PreparedStatement_Reusable()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE k (id INTEGER, v TEXT)");

        using var insert = db.Prepare("INSERT INTO k VALUES (?, ?)");
        for (int i = 0; i < 5; i++)
        {
            insert.Execute(i, $"row-{i}");
        }

        using var select = db.Prepare("SELECT v FROM k WHERE id = ?");
        var r = select.Query(3);
        Assert.Equal(1, r.RowCount);
        Assert.Equal("row-3", r[0, 0]);
    }

    [Fact]
    public void Transaction_Commit_PersistsChanges()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");

        using (var tx = db.Begin())
        {
            tx.Execute("INSERT INTO t VALUES (?)", 1);
            tx.Execute("INSERT INTO t VALUES (?)", 2);
            tx.Commit();
        }

        var r = db.Query("SELECT COUNT(*) FROM t");
        Assert.Equal(2L, r[0, 0]);
    }

    [Fact]
    public void Transaction_Rollback_DiscardsChanges()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        db.Execute("INSERT INTO t VALUES (?)", 100);

        using (var tx = db.Begin())
        {
            tx.Execute("INSERT INTO t VALUES (?)", 200);
            tx.Rollback();
        }

        var r = db.Query("SELECT COUNT(*) FROM t");
        Assert.Equal(1L, r[0, 0]);
    }

    [Fact]
    public void Transaction_DisposeWithoutCommit_RollsBack()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");

        using (var tx = db.Begin())
        {
            tx.Execute("INSERT INTO t VALUES (?)", 1);
            // No Commit; Dispose triggers rollback.
        }

        var r = db.Query("SELECT COUNT(*) FROM t");
        Assert.Equal(0L, r[0, 0]);
    }

    [Fact]
    public void NullParameter_BoundAsNull()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER, s TEXT)");
        db.Execute("INSERT INTO t VALUES (?, ?)", 1, null);
        var r = db.Query("SELECT s FROM t WHERE i = ?", 1);
        Assert.Null(r[0, 0]);
    }

    [Fact]
    public void Clone_SharesEngine()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        db.Execute("INSERT INTO t VALUES (?)", 7);

        using var clone = db.Clone();
        var r = clone.Query("SELECT i FROM t");
        Assert.Equal(7L, r[0, 0]);
    }
}
