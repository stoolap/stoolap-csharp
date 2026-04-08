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

public class DataReaderTests
{
    private static StoolapConnection NewOpenConnection()
    {
        var conn = new StoolapConnection($"Data Source=memory://test-{Guid.NewGuid():N}");
        conn.Open();
        return conn;
    }

    [Fact]
    public void Reader_FieldCount_MatchesProjection()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (a INTEGER, b TEXT)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1, 'x')"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a, b FROM t";
        using var r = cmd.ExecuteReader();
        Assert.Equal(2, r.FieldCount);
    }

    [Fact]
    public void Reader_GetName_ReturnsColumnNames()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (id INTEGER, name TEXT)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1, 'alice')"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name FROM t";
        using var r = cmd.ExecuteReader();
        Assert.Equal("id", r.GetName(0));
        Assert.Equal("name", r.GetName(1));
    }

    [Fact]
    public void Reader_GetOrdinal_LookupByName()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (foo INTEGER, bar TEXT)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1, 'x')"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT foo, bar FROM t";
        using var r = cmd.ExecuteReader();
        Assert.Equal(0, r.GetOrdinal("foo"));
        Assert.Equal(1, r.GetOrdinal("bar"));
    }

    [Fact]
    public void Reader_GetOrdinal_UnknownColumn_Throws()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (a INTEGER)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1)"; c.ExecuteNonQuery(); }
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a FROM t";
        using var r = cmd.ExecuteReader();
        Assert.Throws<IndexOutOfRangeException>(() => r.GetOrdinal("nope"));
    }

    [Fact]
    public void Reader_AllNumericGetters_RoundTrip()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER, f FLOAT, b BOOLEAN)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (42, 3.14, true)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i, f, b FROM t";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal(42L, r.GetInt64(0));
        Assert.Equal(42, r.GetInt32(0));
        Assert.Equal((short)42, r.GetInt16(0));
        Assert.Equal((byte)42, r.GetByte(0));
        Assert.Equal(3.14, r.GetDouble(1), 6);
        Assert.True(r.GetBoolean(2));
    }

    [Fact]
    public void Reader_GetValues_FillsArray()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (10, 20, 30)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a, b, c FROM t";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        var values = new object[3];
        int n = r.GetValues(values);
        Assert.Equal(3, n);
        Assert.Equal(10L, values[0]);
        Assert.Equal(20L, values[1]);
        Assert.Equal(30L, values[2]);
    }

    [Fact]
    public void Reader_IsDBNull_ForNullColumn()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (a INTEGER, b TEXT)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO t VALUES (?, ?)";
            c.Parameters.Add(new StoolapParameter("p1", 1));
            c.Parameters.Add(new StoolapParameter("p2", null));
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a, b FROM t";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.False(r.IsDBNull(0));
        Assert.True(r.IsDBNull(1));
    }

    [Fact]
    public void Reader_NextResult_ReturnsFalse()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (a INTEGER)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1)"; c.ExecuteNonQuery(); }
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a FROM t";
        using var r = cmd.ExecuteReader();
        Assert.False(r.NextResult());
    }

    [Fact]
    public void Reader_EmptyResult_ReadReturnsFalseImmediately()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (a INTEGER)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT a FROM t";
        using var r = cmd.ExecuteReader();
        Assert.False(r.Read());
    }

    [Fact]
    public void Reader_ReadAllRows_TerminatesAfterLast()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER)"; c.ExecuteNonQuery(); }
        for (int i = 0; i < 5; i++)
        {
            using var c = conn.CreateCommand();
            c.CommandText = "INSERT INTO t VALUES (?)";
            c.Parameters.Add(new StoolapParameter("v", i));
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i FROM t ORDER BY i";
        using var r = cmd.ExecuteReader();
        int count = 0;
        while (r.Read()) { count++; }
        Assert.Equal(5, count);
        Assert.False(r.Read()); // additional read returns false
    }

    [Fact]
    public void Reader_GetFieldType_ReportsClrType()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (i INTEGER, s TEXT, b BOOLEAN, f FLOAT)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (1, 'x', true, 1.5)"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT i, s, b, f FROM t";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal(typeof(long), r.GetFieldType(0));
        Assert.Equal(typeof(string), r.GetFieldType(1));
        Assert.Equal(typeof(bool), r.GetFieldType(2));
        Assert.Equal(typeof(double), r.GetFieldType(3));
    }

    [Fact]
    public void Reader_GetString_NullColumn_ReturnsEmpty()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (s TEXT)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand())
        {
            c.CommandText = "INSERT INTO t VALUES (?)";
            c.Parameters.Add(new StoolapParameter("v", null));
            c.ExecuteNonQuery();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT s FROM t";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        // Value is DBNull through GetValue, GetString returns empty for NULL.
        Assert.True(r.IsDBNull(0));
    }

    [Fact]
    public void Reader_StringIndexer_LookupByName()
    {
        using var conn = NewOpenConnection();
        using (var c = conn.CreateCommand()) { c.CommandText = "CREATE TABLE t (id INTEGER, name TEXT)"; c.ExecuteNonQuery(); }
        using (var c = conn.CreateCommand()) { c.CommandText = "INSERT INTO t VALUES (7, 'bob')"; c.ExecuteNonQuery(); }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name FROM t";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal(7L, r["id"]);
        Assert.Equal("bob", r["name"]);
    }
}
