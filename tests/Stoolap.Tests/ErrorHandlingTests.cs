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

public class ErrorHandlingTests
{
    [Fact]
    public void InvalidSql_ThrowsStoolapException()
    {
        using var db = Database.OpenInMemory();
        var ex = Assert.Throws<StoolapException>(() => db.Execute("THIS IS NOT SQL"));
        Assert.False(string.IsNullOrWhiteSpace(ex.Message));
    }

    [Fact]
    public void QueryNonExistentTable_Throws()
    {
        using var db = Database.OpenInMemory();
        Assert.Throws<StoolapException>(() => db.Query("SELECT * FROM nope"));
    }

    [Fact]
    public void DuplicateTable_ThrowsOnSecondCreate()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (id INTEGER)");
        Assert.Throws<StoolapException>(() => db.Execute("CREATE TABLE t (id INTEGER)"));
    }

    [Fact]
    public void Database_DisposedThenUsed_Throws()
    {
        var db = Database.OpenInMemory();
        db.Dispose();
        Assert.Throws<ObjectDisposedException>(() => db.Execute("SELECT 1"));
    }

    [Fact]
    public void PreparedStatement_DisposedThenUsed_Throws()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        var stmt = db.Prepare("INSERT INTO t VALUES (?)");
        stmt.Dispose();
        Assert.Throws<ObjectDisposedException>(() => stmt.Execute(1));
    }

    [Fact]
    public void Rows_DisposedThenRead_Throws()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        db.Execute("INSERT INTO t VALUES (1)");
        var rows = db.QueryStream("SELECT i FROM t");
        rows.Dispose();
        Assert.Throws<ObjectDisposedException>(() => rows.Read());
    }

    [Fact]
    public void Rows_AccessBeforeRead_Throws()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        db.Execute("INSERT INTO t VALUES (1)");
        using var rows = db.QueryStream("SELECT i FROM t");
        Assert.Throws<InvalidOperationException>(() => rows.GetInt64(0));
    }

    [Fact]
    public void Transaction_AfterCommit_Throws()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        var tx = db.Begin();
        tx.Execute("INSERT INTO t VALUES (1)");
        tx.Commit();
        Assert.Throws<InvalidOperationException>(() => tx.Execute("INSERT INTO t VALUES (2)"));
    }

    [Fact]
    public void Transaction_AfterRollback_Throws()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        var tx = db.Begin();
        tx.Execute("INSERT INTO t VALUES (1)");
        tx.Rollback();
        Assert.Throws<InvalidOperationException>(() => tx.Execute("INSERT INTO t VALUES (2)"));
    }

    [Fact]
    public void Transaction_DoubleRollback_Idempotent()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        var tx = db.Begin();
        tx.Rollback();
        tx.Rollback(); // second call is a no-op
    }

    [Fact]
    public void Execute_NullSql_Throws()
    {
        using var db = Database.OpenInMemory();
        Assert.Throws<ArgumentNullException>(() => db.Execute(null!));
    }

    [Fact]
    public void Query_NullSql_Throws()
    {
        using var db = Database.OpenInMemory();
        Assert.Throws<ArgumentNullException>(() => db.Query(null!));
    }

    [Fact]
    public void Execute_NullParameters_Throws()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        Assert.Throws<ArgumentNullException>(() => db.Execute("INSERT INTO t VALUES (?)", (object?[])null!));
    }

    [Fact]
    public void StoolapException_StoresStatusCode()
    {
        var ex = new StoolapException("test", 42);
        Assert.Equal(42, ex.StatusCode);
        Assert.Equal("test", ex.Message);
    }
}
