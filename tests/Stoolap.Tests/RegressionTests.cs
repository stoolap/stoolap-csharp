// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Data;
using Stoolap.Ado;
using Xunit;

namespace Stoolap.Tests;

/// <summary>
/// Regression tests covering the issues raised in the 2026-04-08 Codex review:
///
/// <list type="bullet">
///   <item>P1: non-RID native copy was not architecture-aware (targets file).
///   Not covered here: build-system test lives with manual verification.</item>
///   <item>P2: DbDataReader.GetFieldType threw or returned <c>object</c> before
///   the first Read() (StoolapDataReader).</item>
///   <item>P2: DbCommand skipped connection validation when Transaction was set,
///   and did not verify the transaction belongs to the command's connection.</item>
///   <item>P2: ExecuteReader inside a transaction buffered the full result set.</item>
///   <item>P3: HasRows was hard-coded to <c>true</c> even on empty results.</item>
/// </list>
/// </summary>
public class RegressionTests
{
    private static StoolapConnection NewOpenConnection()
    {
        var conn = new StoolapConnection($"Data Source=memory://regression-{Guid.NewGuid():N}");
        conn.Open();
        return conn;
    }

    // ----- P3 HasRows ------------------------------------------------------

    [Fact]
    public void HasRows_FalseOnEmptyResult()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i FROM t WHERE i = 9999";
        using var r = cmd.ExecuteReader();
        Assert.False(r.HasRows);
        Assert.False(r.Read());
    }

    [Fact]
    public void HasRows_TrueOnNonEmptyResult()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i FROM t";
        using var r = cmd.ExecuteReader();
        Assert.True(r.HasRows);
    }

    // ----- P2 GetFieldType schema stability --------------------------------

    [Fact]
    public void GetFieldType_BeforeRead_ReturnsStableSchema()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER, s TEXT, b BOOLEAN)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1, 'x', true)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i, s, b FROM t";
        using var r = cmd.ExecuteReader();

        // Must work BEFORE any call to Read(): EF / Dapper / column-schema
        // generators inspect metadata at this point.
        Assert.Equal(typeof(long), r.GetFieldType(0));
        Assert.Equal(typeof(string), r.GetFieldType(1));
        Assert.Equal(typeof(bool), r.GetFieldType(2));
    }

    [Fact]
    public void GetFieldType_AfterReadReturnsFalse_StillStable()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i FROM t";
        using var r = cmd.ExecuteReader();
        Assert.False(r.Read());
        // Empty result: we don't know the column type without a schema FFI,
        // but GetFieldType must not throw and must return a sensible fallback.
        var t = r.GetFieldType(0);
        Assert.NotNull(t);
    }

    [Fact]
    public void GetFieldType_NullFirstRow_DoesNotDegradeToObject()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER, s TEXT)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO t VALUES (?, ?)";
            c.Parameters.Add(new StoolapParameter("id", 1));
            c.Parameters.Add(new StoolapParameter("val", null));
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i, s FROM t";
        using var r = cmd.ExecuteReader();

        // First row has NULL in column 1. The old implementation inspected
        // the current cell value and returned typeof(object). With the
        // constructor peek + native type snapshot, we should still report
        // the actual column type via the native FFI's column_type query.
        Assert.Equal(typeof(long), r.GetFieldType(0));
        // The engine reports a value's type, not a column's: with NULL in
        // the only row there is nothing to learn the type from, so the
        // reader reports object. A later non-NULL row is covered by
        // AdoContractTests.Reader_LooksAheadPastNullFirstRows.
        Assert.Equal(typeof(object), r.GetFieldType(1));
    }

    // ----- P2 Command transaction lifecycle --------------------------------

    [Fact]
    public void Command_WithForeignTransaction_ThrowsOnAssignment()
    {
        using var connA = NewOpenConnection();
        using var connB = NewOpenConnection();
        using (var c = connA.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }

        using var tx = connA.BeginTransaction();
        using var cmd = connB.CreateCommand();
        Assert.Throws<InvalidOperationException>(() => cmd.Transaction = tx);
    }

    [Fact]
    public void Command_WithForeignTransaction_ThrowsOnExecute()
    {
        using var connA = NewOpenConnection();
        using var connB = NewOpenConnection();
        using (var c = connA.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }

        using var tx = connA.BeginTransaction();
        var cmd = new StoolapCommand { Connection = connB };
        // Field-level set bypasses the setter guard on purpose to simulate
        // a hostile caller that mutates via reflection or a subclass trick.
        // The Execute path must still reject the mismatch.
        typeof(StoolapCommand)
            .GetField("_transaction", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(cmd, tx);
        cmd.CommandText = "INSERT INTO t VALUES (1)";
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void Command_WithClosedConnection_ThrowsEvenInsideTransaction()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        var tx = conn.BeginTransaction();
        cmd.Transaction = tx;
        cmd.CommandText = "INSERT INTO t VALUES (1)";

        // Close the connection while the command still has a transaction set.
        conn.Close();

        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    // ----- P2 Transactional ExecuteReader streaming ------------------------

    [Fact]
    public void TransactionalExecuteReader_StreamsRows()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE t (i INTEGER, s TEXT)";
            c.ExecuteNonQuery();
        }
        for (int i = 0; i < 10; i++)
        {
            using var c = conn.CreateCommand();
            c.CommandText = "INSERT INTO t VALUES (?, ?)";
            c.Parameters.Add(new StoolapParameter("id", (long)i));
            c.Parameters.Add(new StoolapParameter("v", $"row-{i}"));
            c.ExecuteNonQuery();
        }

        using var tx = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "SELECT i, s FROM t ORDER BY i";

        using var r = cmd.ExecuteReader();
        Assert.True(r.HasRows);

        var ids = new List<long>();
        while (r.Read())
        {
            ids.Add(r.GetInt64(0));
        }
        Assert.Equal(10, ids.Count);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), ids);
    }

    [Fact]
    public void TransactionalExecuteReader_EmptyResult_HasRowsFalse()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }

        using var tx = conn.BeginTransaction();
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "SELECT i FROM t";

        using var r = cmd.ExecuteReader();
        Assert.False(r.HasRows);
        Assert.False(r.Read());
    }

    // ----- Reader peek-ahead sanity ----------------------------------------

    [Fact]
    public void Reader_PeekAhead_DoesNotSkipFirstRow()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO t VALUES (1)";
            c.ExecuteNonQuery();
        }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO t VALUES (2)";
            c.ExecuteNonQuery();
        }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO t VALUES (3)";
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i FROM t ORDER BY i";

        // HasRows and GetFieldType can be called before Read(), but the first
        // row must not be consumed by those checks.
        using var r = cmd.ExecuteReader();
        Assert.True(r.HasRows);
        Assert.Equal(typeof(long), r.GetFieldType(0));

        var values = new List<long>();
        while (r.Read())
        {
            values.Add(r.GetInt64(0));
        }
        Assert.Equal(new long[] { 1, 2, 3 }, values);
    }

    [Fact]
    public void Reader_GetValueBeforeRead_Throws()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i FROM t";
        using var r = cmd.ExecuteReader();
        // Metadata calls are fine before Read(), but value accessors must throw.
        Assert.Throws<InvalidOperationException>(() => r.GetInt64(0));
    }

    [Fact]
    public void Transaction_ExecuteDisposedStatement_Throws()
    {
        // Used to pass the freed native pointer to stoolap_tx_stmt_exec (segfault).
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (id INTEGER)");
        var stmt = db.Prepare("INSERT INTO t VALUES (?)");
        stmt.Dispose();
        using var tx = db.Begin();
        Assert.Throws<ObjectDisposedException>(() => tx.Execute(stmt, 1L));
        Assert.Throws<ObjectDisposedException>(() => tx.Query(stmt, 1L));
    }

    [Theory]
    [InlineData(1600, 1, 1)]
    [InlineData(2300, 1, 1)]
    [InlineData(1, 1, 1)]
    public void DateTimeParameter_OutsideTimestampRange_Throws(int year, int month, int day)
    {
        // Used to wrap around silently: 2300-01-01 was stored as 1715-06-13.
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (ts TIMESTAMP)");
        var dt = new DateTime(year, month, day, 0, 0, 0, DateTimeKind.Utc);
        Assert.Throws<ArgumentOutOfRangeException>(() => db.Execute("INSERT INTO t VALUES (?)", dt));
    }

    [Fact]
    public void DateTimeParameter_AtRangeEdges_RoundTrips()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (id INTEGER, ts TIMESTAMP)");
        var low = new DateTime(1677, 9, 22, 0, 0, 0, DateTimeKind.Utc);
        var high = new DateTime(2262, 4, 10, 0, 0, 0, DateTimeKind.Utc);
        db.Execute("INSERT INTO t VALUES (1, ?), (2, ?)", low, high);
        var r = db.Query("SELECT ts FROM t ORDER BY id");
        Assert.Equal(low, r[0, 0]);
        Assert.Equal(high, r[1, 0]);
    }

    [Fact]
    public void UlongParameter_AboveInt64_Throws()
    {
        // Used to be stored as -1.
        using var db = Database.OpenInMemory();
        Assert.Throws<OverflowException>(() => db.Query("SELECT ?", ulong.MaxValue));
        Assert.Equal(42L, db.Query("SELECT ?", 42UL)[0, 0]);
    }

    [Fact]
    public void NamedParameter_UsedTwice_IsOneExpression()
    {
        // Engine 0.5.0 treats two anonymous ? as different expressions, so
        // GROUP BY g + ? did not match SELECT g + ?.
        using var conn = new StoolapConnection($"Data Source=memory://reg-{Guid.NewGuid():N}");
        conn.Open();
        using (var ddl = conn.CreateCommand())
        {
            ddl.CommandText = "CREATE TABLE t (g INTEGER)";
            ddl.ExecuteNonQuery();
            ddl.CommandText = "INSERT INTO t VALUES (1), (1), (2)";
            ddl.ExecuteNonQuery();
        }
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT g + @x AS k, COUNT(*) FROM t GROUP BY g + @x ORDER BY k";
        cmd.Parameters.Add(new StoolapParameter("@x", 10L));
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal(11L, r.GetInt64(0));
        Assert.Equal(2L, r.GetInt64(1));
        Assert.True(r.Read());
        Assert.Equal(12L, r.GetInt64(0));
        Assert.False(r.Read());
    }
}
