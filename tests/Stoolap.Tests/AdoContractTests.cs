// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Data;
using Dapper;
using Stoolap.Ado;
using Xunit;

namespace Stoolap.Tests;

/// <summary>
/// ADO.NET contract behaviour that Dapper and similar libraries rely on:
/// schema types before Read, connection close semantics, CloseConnection,
/// RecordsAffected, ordinal and NULL handling.
/// </summary>
public class AdoContractTests
{
    public record User(long Id, string Name, string? Email);

    public class Person
    {
        public long Id { get; set; }
        public int? Age { get; set; }
    }

    private static StoolapConnection OpenUnique()
    {
        var conn = new StoolapConnection($"Data Source=memory://contract-{Guid.NewGuid():N}");
        conn.Open();
        return conn;
    }

    private static StoolapConnection SeedUsers(params (long id, string name, string? email)[] users)
    {
        var conn = OpenUnique();
        conn.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
        foreach (var u in users)
        {
            conn.Execute("INSERT INTO users VALUES (@id, @name, @email)", new { u.id, u.name, u.email });
        }
        return conn;
    }

    [Fact]
    public void Dapper_RecordMapping_WithNullInFirstRow()
    {
        using var conn = SeedUsers((1, "alice", null), (2, "bob", "bob@example.com"));
        var users = conn.Query<User>("SELECT id, name, email FROM users ORDER BY id").ToList();
        Assert.Equal(2, users.Count);
        Assert.Null(users[0].Email);
        Assert.Equal("bob@example.com", users[1].Email);
    }

    [Fact]
    public void Dapper_NullableProperty_WithNullInFirstRow()
    {
        using var conn = OpenUnique();
        conn.Execute("CREATE TABLE p (id INTEGER PRIMARY KEY, age INTEGER)");
        conn.Execute("INSERT INTO p VALUES (1, NULL), (2, 30)");
        var people = conn.Query<Person>("SELECT id, age FROM p ORDER BY id").ToList();
        Assert.Null(people[0].Age);
        Assert.Equal(30, people[1].Age);
    }

    [Fact]
    public void Dapper_ScalarAndExecute()
    {
        using var conn = SeedUsers((1, "alice", "a@example.com"));
        Assert.Equal(1, conn.Execute("UPDATE users SET name = @n WHERE id = @id", new { n = "ally", id = 1L }));
        Assert.Equal("ally", conn.ExecuteScalar<string>("SELECT name FROM users WHERE id = @id", new { id = 1L }));
    }

    [Fact]
    public void Dapper_OpensAndClosesAClosedConnection()
    {
        using var conn = SeedUsers((1, "alice", "a@example.com"));
        // A memory:// engine lives while any handle on its DSN is open.
        using var keepAlive = new StoolapConnection(conn.ConnectionString);
        keepAlive.Open();
        conn.Close();

        // Dapper opens a closed connection and relies on CloseConnection to close it again.
        Assert.Equal(1L, conn.Query<long>("SELECT id FROM users").Single());
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public void Reader_LooksAheadPastNullFirstRows()
    {
        using var conn = OpenUnique();
        conn.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        for (int i = 1; i <= 10; i++)
        {
            conn.Execute("INSERT INTO t VALUES (@id, @v)", new { id = (long)i, v = i <= 5 ? null : "x" + i });
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, v FROM t ORDER BY id";
        using var r = cmd.ExecuteReader();
        Assert.Equal(typeof(string), r.GetFieldType(1));

        // Every row is still returned, in order, with the right values.
        for (int i = 1; i <= 10; i++)
        {
            Assert.True(r.Read());
            Assert.Equal(i, r.GetInt64(0));
            Assert.Equal(i <= 5, r.IsDBNull(1));
            if (i > 5)
            {
                Assert.Equal("x" + i, r.GetString(1));
            }
        }
        Assert.False(r.Read());
    }

    [Fact]
    public void Reader_LookAheadIsBounded_AndStreamingResumes()
    {
        int n = StoolapDataReader.TypeLookAheadRows + 50;
        using var conn = OpenUnique();
        conn.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        using (var tx = conn.BeginTransaction())
        {
            for (int i = 1; i <= n; i++)
            {
                conn.Execute("INSERT INTO t VALUES (@id, NULL)", new { id = (long)i }, tx);
            }
            tx.Commit();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, v FROM t ORDER BY id";
        using var r = cmd.ExecuteReader();
        Assert.Equal(typeof(object), r.GetFieldType(1)); // never non-NULL within the window
        int seen = 0;
        while (r.Read())
        {
            seen++;
            Assert.Equal(seen, r.GetInt64(0));
            Assert.True(r.IsDBNull(1));
        }
        Assert.Equal(n, seen);
    }

    [Fact]
    public void Reader_TypedGetterOnNull_ThrowsInvalidCast()
    {
        using var conn = SeedUsers((1, "alice", null));
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, email FROM users";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Throws<InvalidCastException>(() => r.GetString(1));
        Assert.Equal(DBNull.Value, r.GetValue(1));
    }

    [Fact]
    public void Reader_TypedGetterOnNull_InReadAheadRow_ThrowsInvalidCast()
    {
        using var conn = SeedUsers((1, "alice", null), (2, "bob", "b@example.com"));
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, email FROM users ORDER BY id";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Throws<InvalidCastException>(() => r.GetString(1));
        Assert.Equal(1, r.GetInt32(0));
    }

    [Fact]
    public void Reader_OrdinalOutOfRange_Throws()
    {
        using var conn = SeedUsers((1, "alice", "a@example.com"));
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM users";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Throws<IndexOutOfRangeException>(() => r.GetInt64(1));
        Assert.Throws<IndexOutOfRangeException>(() => r.GetValue(-1));
        Assert.Throws<IndexOutOfRangeException>(() => r.IsDBNull(5));
        Assert.Throws<IndexOutOfRangeException>(() => r.GetName(1));
        Assert.Throws<IndexOutOfRangeException>(() => r.GetFieldType(1));
    }

    [Fact]
    public void Reader_RecordsAffected_FollowsAdoConvention()
    {
        using var conn = SeedUsers((1, "alice", "a@example.com"), (2, "bob", "b@example.com"));
        using (var q = conn.CreateCommand())
        {
            q.CommandText = "SELECT id FROM users";
            using var r = q.ExecuteReader();
            Assert.Equal(-1, r.RecordsAffected);
        }
        using (var u = conn.CreateCommand())
        {
            u.CommandText = "UPDATE users SET name = 'x'";
            var r = u.ExecuteReader();
            r.Close();
            Assert.Equal(2, r.RecordsAffected); // still readable after Close
        }
    }

    [Fact]
    public void Close_RollsBackOpenTransaction_AndClosesReaders()
    {
        using var conn = SeedUsers((1, "alice", "a@example.com"));
        using var keepAlive = new StoolapConnection(conn.ConnectionString);
        keepAlive.Open();

        var tx = conn.BeginTransaction();
        conn.Execute("INSERT INTO users VALUES (2, 'bob', NULL)", transaction: tx);
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM users";
        var reader = cmd.ExecuteReader();

        conn.Close();

        Assert.True(reader.IsClosed);
        Assert.Throws<InvalidOperationException>(() => tx.Commit());
        Assert.Equal(1L, keepAlive.ExecuteScalar<long>("SELECT COUNT(*) FROM users"));
    }

    [Fact]
    public void ExecuteReader_CloseConnection_ClosesItOnReaderClose()
    {
        using var conn = SeedUsers((1, "alice", "a@example.com"));
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id FROM users";
        using (var r = cmd.ExecuteReader(CommandBehavior.CloseConnection))
        {
            Assert.True(r.Read());
        }
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public void Command_NamedPlaceholderWithoutParameters_Throws()
    {
        using var conn = OpenUnique();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CAST(@a AS INTEGER)";
        var ex = Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
        Assert.Contains("'a'", ex.Message);
    }

    [Fact]
    public void Rows_ColumnOutOfRange_Throws()
    {
        using var db = Database.OpenInMemory();
        using var rows = db.QueryStream("SELECT 1");
        Assert.True(rows.Read());
        Assert.Throws<ArgumentOutOfRangeException>(() => rows.GetInt64(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => rows.GetValue(-1));
    }

    private enum Color : long { Red = 1, Blue = 7 }

    [Fact]
    public void Parameters_EnumCharByteAndUnknownTypes()
    {
        using var db = Database.OpenInMemory();
        Assert.Equal(7L, db.Query("SELECT ?", Color.Blue)[0, 0]);
        Assert.Equal(5L, db.Query("SELECT ?", DayOfWeek.Friday)[0, 0]);
        Assert.Equal("z", db.Query("SELECT ?", 'z')[0, 0]);

        var vector = new byte[8];
        BitConverter.TryWriteBytes(vector.AsSpan(0, 4), 1.5f);
        BitConverter.TryWriteBytes(vector.AsSpan(4, 4), -2f);
        Assert.Equal(new[] { 1.5f, -2f }, (float[])db.Query("SELECT ?", vector)[0, 0]!);

        Assert.Throws<ArgumentException>(() => db.Query("SELECT ?", new byte[5]));
        Assert.Throws<NotSupportedException>(() => db.Query("SELECT ?", TimeSpan.FromSeconds(1)));
        Assert.Throws<NotSupportedException>(() => db.Query("SELECT ?", new[] { 1, 2 }));
    }

    [Fact]
    public void Reader_GetFieldValue_ConvertsThroughTypedGetters()
    {
        using var conn = SeedUsers((1, "alice", null));
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, email FROM users";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal(1, r.GetFieldValue<int>(0));
        Assert.Equal(1L, r.GetFieldValue<long>(0));
        Assert.Equal(1, r.GetFieldValue<int?>(0));
        Assert.Equal("alice", r.GetFieldValue<string>(1));
        Assert.Null(r.GetFieldValue<string?>(2));
        Assert.Null(r.GetFieldValue<int?>(2));
        Assert.Throws<InvalidCastException>(() => r.GetFieldValue<int>(2));
    }

    [Fact]
    public void Reader_GetSchemaTable_LoadsIntoDataTable()
    {
        using var conn = SeedUsers((1, "alice", "a@example.com"), (2, "bob", null));
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, email FROM users ORDER BY id";
        using var r = cmd.ExecuteReader();

        var schema = r.GetSchemaTable()!;
        Assert.Equal(3, schema.Rows.Count);
        Assert.Equal("id", schema.Rows[0]["ColumnName"]);
        Assert.Equal(typeof(long), schema.Rows[0]["DataType"]);
        Assert.Equal(typeof(string), schema.Rows[2]["DataType"]);

        var table = new DataTable();
        table.Load(r);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal("bob", table.Rows[1]["name"]);
        Assert.Equal(DBNull.Value, table.Rows[1]["email"]);
    }

    [Theory]
    [InlineData("SELECT @a + ?")]
    [InlineData("SELECT $1, @a")]
    public void Command_MixedNamedAndPositional_Throws(string sql)
    {
        // The $n rewrite would otherwise give ? or $1 a named parameter's value.
        using var conn = OpenUnique();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new StoolapParameter("@a", 7L));
        cmd.Parameters.Add(new StoolapParameter("@b", 8L));
        var ex = Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
        Assert.Contains("mixes named", ex.Message);
    }

    [Fact]
    public void Reader_TypedGetters_SameRuleForReadAheadAndStreamedRows()
    {
        // n is NULL in the first rows, so rows 1..64 are read ahead (boxed)
        // and later rows come straight from the native cursor.
        int total = StoolapDataReader.TypeLookAheadRows + 10;
        using var conn = OpenUnique();
        conn.Execute("CREATE TABLE t (id INTEGER PRIMARY KEY, txt TEXT, ts TIMESTAMP, n INTEGER)");
        using (var tx = conn.BeginTransaction())
        {
            for (int i = 1; i <= total; i++)
            {
                conn.Execute("INSERT INTO t VALUES (@id, 'abc', @ts, @n)",
                    new { id = (long)i, ts = DateTime.UnixEpoch.AddSeconds(i), n = i > total - 5 ? (long?)i : null }, tx);
            }
            tx.Commit();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, txt, ts, n FROM t ORDER BY id";
        using var r = cmd.ExecuteReader();
        int row = 0;
        while (r.Read())
        {
            row++;
            Assert.Equal(row, r.GetInt64(0));
            Assert.Equal((double)row, r.GetDouble(0));
            Assert.Equal(row.ToString(System.Globalization.CultureInfo.InvariantCulture), r.GetString(0));
            Assert.Equal("abc", r.GetString(1));
            Assert.Throws<InvalidCastException>(() => r.GetInt64(1));
            Assert.Throws<InvalidCastException>(() => r.GetDateTime(1));
            Assert.Equal(DateTime.UnixEpoch.AddSeconds(row), r.GetDateTime(2));
            Assert.Throws<InvalidCastException>(() => r.GetString(2));
            Assert.Throws<InvalidCastException>(() => r.GetInt64(2));
        }
        Assert.Equal(total, row);
    }

    [Fact]
    public void Reader_GetFieldValue_ObjectAndUnsignedAndOffset()
    {
        using var conn = SeedUsers((1, "alice", null));
        conn.Execute("CREATE TABLE d (ts TIMESTAMP)");
        conn.Execute("INSERT INTO d VALUES (@ts)", new { ts = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc) });

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT id, email FROM users";
            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(DBNull.Value, r.GetFieldValue<object>(1));
            Assert.Equal(1u, r.GetFieldValue<uint>(0));
            Assert.Equal((ulong)1, r.GetFieldValue<ulong>(0));
        }
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT ts FROM d";
            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), r.GetFieldValue<DateTimeOffset>(0));
        }
    }
}
