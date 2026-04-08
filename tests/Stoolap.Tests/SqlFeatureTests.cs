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

/// <summary>
/// End-to-end tests for SQL features executed through the high-level API.
/// Each test uses a fresh in-memory database (no shared state).
/// </summary>
public class SqlFeatureTests
{
    private static Database SeededUsersDb()
    {
        var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, balance FLOAT)");
        db.Execute("INSERT INTO users VALUES (?, ?, ?, ?)", 1, "alice", 30, 1000.0);
        db.Execute("INSERT INTO users VALUES (?, ?, ?, ?)", 2, "bob", 25, 500.0);
        db.Execute("INSERT INTO users VALUES (?, ?, ?, ?)", 3, "carol", 40, 2000.0);
        db.Execute("INSERT INTO users VALUES (?, ?, ?, ?)", 4, "dave", 22, 750.0);
        db.Execute("INSERT INTO users VALUES (?, ?, ?, ?)", 5, "eve", 35, 1500.0);
        return db;
    }

    [Fact]
    public void Aggregate_Count()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT COUNT(*) FROM users");
        Assert.Equal(5L, r[0, 0]);
    }

    [Fact]
    public void Aggregate_SumAvg()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT SUM(balance), AVG(balance) FROM users");
        // SUM/AVG on FLOAT may come back boxed as a numeric type other than
        // System.Double (e.g. decimal or long for integer sums); normalize
        // through Convert to keep the assertion value-focused.
        Assert.Equal(5750.0, Convert.ToDouble(r[0, 0]), 2);
        Assert.Equal(1150.0, Convert.ToDouble(r[0, 1]), 2);
    }

    [Fact]
    public void Aggregate_MinMax()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT MIN(age), MAX(age) FROM users");
        Assert.Equal(22L, r[0, 0]);
        Assert.Equal(40L, r[0, 1]);
    }

    [Fact]
    public void GroupBy_BucketsRowsCorrectly()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (k TEXT, v INTEGER)");
        db.Execute("INSERT INTO t VALUES ('a', 1)");
        db.Execute("INSERT INTO t VALUES ('a', 2)");
        db.Execute("INSERT INTO t VALUES ('b', 10)");
        db.Execute("INSERT INTO t VALUES ('b', 20)");
        db.Execute("INSERT INTO t VALUES ('c', 100)");

        var r = db.Query("SELECT k, SUM(v) FROM t GROUP BY k ORDER BY k");
        Assert.Equal(3, r.RowCount);
        // Normalize SUM result via Convert — the boxed type depends on
        // which aggregate implementation path the planner chose.
        var buckets = new Dictionary<string, long>();
        for (int i = 0; i < r.RowCount; i++)
        {
            buckets[(string)r[i, 0]!] = Convert.ToInt64(r[i, 1]);
        }
        Assert.Equal(3L, buckets["a"]);
        Assert.Equal(30L, buckets["b"]);
        Assert.Equal(100L, buckets["c"]);
    }

    [Fact]
    public void GroupByHaving_FiltersGroups()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (k TEXT, v INTEGER)");
        for (int i = 0; i < 5; i++) db.Execute("INSERT INTO t VALUES ('a', ?)", i);
        for (int i = 0; i < 2; i++) db.Execute("INSERT INTO t VALUES ('b', ?)", i);

        var r = db.Query("SELECT k FROM t GROUP BY k HAVING COUNT(*) > 3");
        Assert.Equal(1, r.RowCount);
        Assert.Equal("a", r[0, 0]);
    }

    [Fact]
    public void OrderBy_AscDesc()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT name FROM users ORDER BY age DESC");
        Assert.Equal("carol", r[0, 0]);
        Assert.Equal("dave", r[4, 0]);
    }

    [Fact]
    public void LimitOffset_PaginatesCorrectly()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 2");
        Assert.Equal(2, r.RowCount);
        Assert.Equal(3L, r[0, 0]);
        Assert.Equal(4L, r[1, 0]);
    }

    [Fact]
    public void InnerJoin_TwoTables()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE u (id INTEGER, name TEXT)");
        db.Execute("CREATE TABLE o (id INTEGER, user_id INTEGER, amt INTEGER)");
        db.Execute("INSERT INTO u VALUES (1, 'a')");
        db.Execute("INSERT INTO u VALUES (2, 'b')");
        db.Execute("INSERT INTO o VALUES (10, 1, 100)");
        db.Execute("INSERT INTO o VALUES (11, 1, 200)");
        db.Execute("INSERT INTO o VALUES (12, 2, 50)");

        var r = db.Query("SELECT u.name, SUM(o.amt) FROM u INNER JOIN o ON u.id = o.user_id GROUP BY u.id, u.name");
        Assert.Equal(2, r.RowCount);

        // Lookup-based assertion: don't depend on join/group-by output order.
        var byName = new Dictionary<string, long>();
        for (int i = 0; i < r.RowCount; i++)
        {
            byName[(string)r[i, 0]!] = Convert.ToInt64(r[i, 1]);
        }
        Assert.Equal(300L, byName["a"]);
        Assert.Equal(50L, byName["b"]);
    }

    [Fact]
    public void LeftJoin_PreservesUnmatchedRows()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE u (id INTEGER, name TEXT)");
        db.Execute("CREATE TABLE o (id INTEGER, user_id INTEGER, amt INTEGER)");
        db.Execute("INSERT INTO u VALUES (1, 'a')");
        db.Execute("INSERT INTO u VALUES (2, 'b')");
        db.Execute("INSERT INTO o VALUES (10, 1, 100)");

        // All users must appear in the result (LEFT JOIN contract). For the
        // unmatched user ('b'), o.amt comes back NULL.
        var r = db.Query("SELECT u.name, o.amt FROM u LEFT JOIN o ON u.id = o.user_id");
        Assert.Equal(2, r.RowCount);

        var names = new HashSet<string>();
        for (int i = 0; i < r.RowCount; i++)
        {
            names.Add((string)r[i, 0]!);
        }
        Assert.Contains("a", names);
        Assert.Contains("b", names);
    }

    [Fact]
    public void Distinct_RemovesDuplicates()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (v INTEGER)");
        for (int i = 0; i < 10; i++) db.Execute("INSERT INTO t VALUES (?)", i % 3);

        var r = db.Query("SELECT DISTINCT v FROM t ORDER BY v");
        Assert.Equal(3, r.RowCount);
    }

    [Fact]
    public void In_ListMatch()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT name FROM users WHERE id IN (1, 3, 5) ORDER BY id");
        Assert.Equal(3, r.RowCount);
        Assert.Equal("alice", r[0, 0]);
        Assert.Equal("carol", r[1, 0]);
        Assert.Equal("eve", r[2, 0]);
    }

    [Fact]
    public void Like_PatternMatch()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT name FROM users WHERE name LIKE 'a%'");
        Assert.Equal(1, r.RowCount);
        Assert.Equal("alice", r[0, 0]);
    }

    [Fact]
    public void Cte_WithClause()
    {
        using var db = SeededUsersDb();
        var r = db.Query(@"
            WITH wealthy AS (SELECT * FROM users WHERE balance > 1000)
            SELECT COUNT(*) FROM wealthy");
        Assert.Equal(2L, r[0, 0]);
    }

    [Fact]
    public void Subquery_Scalar()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT name FROM users WHERE balance > (SELECT AVG(balance) FROM users) ORDER BY id");
        Assert.True(r.RowCount >= 1);
    }

    [Fact]
    public void CaseExpression_ReturnsBucket()
    {
        using var db = SeededUsersDb();
        var r = db.Query("SELECT name, CASE WHEN age < 30 THEN 'young' ELSE 'old' END FROM users ORDER BY id");
        Assert.Equal(5, r.RowCount);
        Assert.Equal("old", r[0, 1]);
        Assert.Equal("young", r[1, 1]);
    }

    [Fact]
    public void DropTable_RemovesTable()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER)");
        db.Execute("INSERT INTO t VALUES (1)");
        db.Execute("DROP TABLE t");
        Assert.Throws<StoolapException>(() => db.Query("SELECT * FROM t"));
    }
}
