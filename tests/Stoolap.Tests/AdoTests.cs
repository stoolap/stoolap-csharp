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

public class AdoTests
{
    [Fact]
    public void Connection_Open_AndExecute()
    {
        using var conn = new StoolapConnection("Data Source=memory://");
        conn.Open();
        Assert.Equal(ConnectionState.Open, conn.State);

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (id INTEGER, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO t VALUES (@id, @name)";
            var p1 = cmd.CreateParameter();
            p1.ParameterName = "@id";
            p1.Value = 1;
            cmd.Parameters.Add(p1);

            var p2 = cmd.CreateParameter();
            p2.ParameterName = "@name";
            p2.Value = "alice";
            cmd.Parameters.Add(p2);

            int rows = cmd.ExecuteNonQuery();
            Assert.Equal(1, rows);
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM t WHERE id = @id";
            var p = cmd.CreateParameter();
            p.ParameterName = "@id";
            p.Value = 1;
            cmd.Parameters.Add(p);

            var name = (string?)cmd.ExecuteScalar();
            Assert.Equal("alice", name);
        }
    }

    [Fact]
    public void DataReader_Read_StreamsRows()
    {
        using var conn = new StoolapConnection();
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (i INTEGER, s TEXT)";
            cmd.ExecuteNonQuery();
        }

        for (int i = 0; i < 5; i++)
        {
            using var insert = conn.CreateCommand();
            insert.CommandText = "INSERT INTO t VALUES (@i, @s)";
            insert.Parameters.Add(new StoolapParameter("@i", i));
            insert.Parameters.Add(new StoolapParameter("@s", $"row-{i}"));
            insert.ExecuteNonQuery();
        }

        using var select = conn.CreateCommand();
        select.CommandText = "SELECT i, s FROM t ORDER BY i";
        using var reader = select.ExecuteReader();

        var collected = new List<(long, string)>();
        while (reader.Read())
        {
            collected.Add((reader.GetInt64(0), reader.GetString(1)));
        }
        Assert.Equal(5, collected.Count);
        Assert.Equal((0L, "row-0"), collected[0]);
        Assert.Equal((4L, "row-4"), collected[4]);
    }

    [Fact]
    public void Transaction_RollbackOnDispose()
    {
        using var conn = new StoolapConnection();
        conn.Open();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE t (i INTEGER)";
            cmd.ExecuteNonQuery();
        }

        using (var tx = conn.BeginTransaction())
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO t VALUES (1)";
            cmd.ExecuteNonQuery();
            // No commit; falls out of scope and rolls back.
        }

        using var verify = conn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM t";
        Assert.Equal(0L, verify.ExecuteScalar());
    }
}
