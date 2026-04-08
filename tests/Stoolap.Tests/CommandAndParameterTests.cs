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

public class CommandAndParameterTests
{
    private static StoolapConnection NewOpenConnection()
    {
        // Unique DSN per call so xUnit's parallel test classes do not share
        // engines through Stoolap's global DSN registry.
        var conn = new StoolapConnection($"Data Source=memory://test-{Guid.NewGuid():N}");
        conn.Open();
        return conn;
    }

    [Fact]
    public void CreateCommand_HasConnection()
    {
        using var conn = NewOpenConnection();
        using var cmd = conn.CreateCommand();
        Assert.Same(conn, cmd.Connection);
    }

    [Fact]
    public void Parameter_NameWithoutSigil_StoredAsIs()
    {
        var p = new StoolapParameter("id", 42);
        Assert.Equal("id", p.ParameterName);
        Assert.Equal(42, p.Value);
    }

    [Fact]
    public void Parameter_AtSigilStripped()
    {
        var p = new StoolapParameter("@id", 42);
        Assert.Equal("id", p.ParameterName);
    }

    [Fact]
    public void Parameter_ColonSigilStripped()
    {
        var p = new StoolapParameter(":id", 42);
        Assert.Equal("id", p.ParameterName);
    }

    [Fact]
    public void Parameter_DollarSigilStripped()
    {
        var p = new StoolapParameter("$id", 42);
        Assert.Equal("id", p.ParameterName);
    }

    [Fact]
    public void Parameter_DefaultDirection_IsInput()
    {
        var p = new StoolapParameter();
        Assert.Equal(ParameterDirection.Input, p.Direction);
    }

    [Fact]
    public void Parameter_OutputDirection_Throws()
    {
        var p = new StoolapParameter();
        Assert.Throws<NotSupportedException>(() => p.Direction = ParameterDirection.Output);
    }

    [Fact]
    public void ParameterCollection_AddByName_FoundByLookup()
    {
        var coll = new StoolapParameterCollection();
        coll.AddWithValue("@id", 1);
        Assert.Equal(1, coll.Count);
        Assert.True(coll.Contains("id"));
        Assert.True(coll.Contains("@id"));
    }

    [Fact]
    public void ParameterCollection_IndexOfNormalized_WorksWithOrWithoutSigil()
    {
        var coll = new StoolapParameterCollection();
        coll.AddWithValue("name", "alice");
        Assert.True(coll.IndexOf("name") >= 0);
        Assert.True(coll.IndexOf("@name") >= 0);
        Assert.True(coll.IndexOf(":name") >= 0);
    }

    [Fact]
    public void ParameterCollection_RemoveByName_RemovesEntry()
    {
        var coll = new StoolapParameterCollection();
        coll.AddWithValue("a", 1);
        coll.AddWithValue("b", 2);
        coll.RemoveAt("@a");
        Assert.Equal(1, coll.Count);
        Assert.False(coll.Contains("a"));
        Assert.True(coll.Contains("b"));
    }

    [Fact]
    public void ParameterCollection_Clear_EmptiesCollection()
    {
        var coll = new StoolapParameterCollection();
        coll.AddWithValue("a", 1);
        coll.AddWithValue("b", 2);
        coll.Clear();
        Assert.Equal(0, coll.Count);
    }

    [Fact]
    public void Command_ExecuteNonQuery_NoParameters()
    {
        using var conn = NewOpenConnection();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER)";
            int rc = cmd.ExecuteNonQuery();
            Assert.True(rc >= 0);
        }
    }

    [Fact]
    public void Command_ExecuteScalar_ReturnsFirstColumn()
    {
        using var conn = NewOpenConnection();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO t VALUES (@id, @name)";
            cmd.Parameters.Add(new StoolapParameter("@id", 1));
            cmd.Parameters.Add(new StoolapParameter("@name", "test"));
            cmd.ExecuteNonQuery();
        }
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM t WHERE id = @id";
            cmd.Parameters.Add(new StoolapParameter("@id", 1));
            var v = cmd.ExecuteScalar();
            Assert.Equal("test", v);
        }
    }

    [Fact]
    public void Command_NoParametersWithSigilFreeSql_PositionalFallback()
    {
        // When SQL has no named placeholders but parameters are present,
        // they should be applied positionally in declaration order.
        using var conn = NewOpenConnection();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO t VALUES (?, ?)";
            cmd.Parameters.Add(new StoolapParameter("p1", 5));
            cmd.Parameters.Add(new StoolapParameter("p2", "hello"));
            int rc = cmd.ExecuteNonQuery();
            Assert.Equal(1, rc);
        }

        using (var verify = conn.CreateCommand())
        {
            verify.CommandText = "SELECT name FROM t WHERE id = ?";
            verify.Parameters.Add(new StoolapParameter("anything", 5));
            Assert.Equal("hello", verify.ExecuteScalar());
        }
    }

    [Fact]
    public void Command_MissingNamedParam_Throws()
    {
        using var conn = NewOpenConnection();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO t VALUES (@id)";
            cmd.Parameters.Add(new StoolapParameter("@other", 1));
            Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
        }
    }

    [Fact]
    public void Command_NoCommandText_Throws()
    {
        using var conn = NewOpenConnection();
        using var cmd = conn.CreateCommand();
        cmd.Parameters.Add(new StoolapParameter("@x", 1));
        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void Command_CommandTypeStoredProc_Throws()
    {
        using var conn = NewOpenConnection();
        using var cmd = conn.CreateCommand();
        Assert.Throws<NotSupportedException>(() => cmd.CommandType = CommandType.StoredProcedure);
    }
}
