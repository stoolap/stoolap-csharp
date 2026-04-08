// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using Stoolap;
using Stoolap.Native;
using Xunit;

namespace Stoolap.Tests;

/// <summary>
/// Round-trip tests for every Stoolap type both via the binary fetch-all
/// path (Database.Query) and the streaming reader path (Database.QueryStream).
/// </summary>
public class TypeRoundTripTests
{
    [Fact]
    public void Integer_NegativeAndLarge()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (a INTEGER, b INTEGER)");
        db.Execute("INSERT INTO t VALUES (?, ?)", -9_223_372_036_854_775_807L, 9_223_372_036_854_775_806L);

        var r = db.Query("SELECT a, b FROM t");
        Assert.Equal(-9_223_372_036_854_775_807L, r[0, 0]);
        Assert.Equal(9_223_372_036_854_775_806L, r[0, 1]);
    }

    [Fact]
    public void Float_PrecisionPreserved()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (f FLOAT)");
        db.Execute("INSERT INTO t VALUES (?)", Math.PI);

        var r = db.Query("SELECT f FROM t");
        Assert.Equal(Math.PI, (double)r[0, 0]!, 14);
    }

    [Fact]
    public void Boolean_TrueAndFalse()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (b BOOLEAN)");
        db.Execute("INSERT INTO t VALUES (?)", true);
        db.Execute("INSERT INTO t VALUES (?)", false);

        var r = db.Query("SELECT b FROM t ORDER BY b");
        Assert.Equal(false, r[0, 0]);
        Assert.Equal(true, r[1, 0]);
    }

    [Fact]
    public void Text_Unicode()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (s TEXT)");
        const string unicode = "Selâm dünya 🌍 مرحبا";
        db.Execute("INSERT INTO t VALUES (?)", unicode);

        var r = db.Query("SELECT s FROM t");
        Assert.Equal(unicode, r[0, 0]);
    }

    [Fact]
    public void Text_VeryLong()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (s TEXT)");
        var text = new string('z', 100_000);
        db.Execute("INSERT INTO t VALUES (?)", text);

        var r = db.Query("SELECT s FROM t");
        Assert.Equal(text, r[0, 0]);
    }

    [Fact]
    public void Timestamp_RoundTripsThroughBinaryPath()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (ts TIMESTAMP)");
        var when = new DateTime(2026, 4, 8, 15, 30, 45, DateTimeKind.Utc);
        db.Execute("INSERT INTO t VALUES (?)", when);

        var r = db.Query("SELECT ts FROM t");
        var got = (DateTime)r[0, 0]!;
        Assert.Equal(when, got);
    }

    [Fact]
    public void Timestamp_RoundTripsThroughStreamingPath()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (ts TIMESTAMP)");
        var when = new DateTime(2026, 4, 8, 15, 30, 45, DateTimeKind.Utc);
        db.Execute("INSERT INTO t VALUES (?)", when);

        using var rows = db.QueryStream("SELECT ts FROM t");
        Assert.True(rows.Read());
        Assert.Equal(when, rows.GetDateTime(0));
    }

    [Fact]
    public void Vector_RoundTripsThroughBinaryPath()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (v VECTOR(4))");
        var vec = new float[] { 1f, 2f, 3f, 4f };
        db.Execute("INSERT INTO t VALUES (?)", vec);

        var r = db.Query("SELECT v FROM t");
        var got = (float[])r[0, 0]!;
        Assert.Equal(vec, got);
    }

    [Fact]
    public void Vector_RoundTripsThroughStreamingPath()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (v VECTOR(3))");
        var vec = new float[] { 0.5f, 1.5f, 2.5f };
        db.Execute("INSERT INTO t VALUES (?)", vec);

        using var rows = db.QueryStream("SELECT v FROM t");
        Assert.True(rows.Read());
        Assert.Equal(vec, rows.GetVector(0));
    }

    [Fact]
    public void Json_RoundTrips()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (j JSON)");
        db.Execute("INSERT INTO t VALUES (?)", "{\"a\": 1, \"b\": [2, 3]}");

        var r = db.Query("SELECT j FROM t");
        Assert.Equal("{\"a\": 1, \"b\": [2, 3]}", r[0, 0]);
    }

    [Fact]
    public void Null_AllTypesAcceptNull()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN, ts TIMESTAMP)");
        db.Execute("INSERT INTO t VALUES (?, ?, ?, ?, ?)", null, null, null, null, null);

        var r = db.Query("SELECT i, f, s, b, ts FROM t");
        Assert.Null(r[0, 0]);
        Assert.Null(r[0, 1]);
        Assert.Null(r[0, 2]);
        Assert.Null(r[0, 3]);
        Assert.Null(r[0, 4]);
    }

    [Fact]
    public void Streaming_FieldType_MatchesEnum()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER, s TEXT, b BOOLEAN, f FLOAT)");
        db.Execute("INSERT INTO t VALUES (1, 'x', true, 1.5)");

        using var rows = db.QueryStream("SELECT i, s, b, f FROM t");
        Assert.True(rows.Read());
        Assert.Equal(StoolapType.Integer, rows.GetFieldType(0));
        Assert.Equal(StoolapType.Text, rows.GetFieldType(1));
        Assert.Equal(StoolapType.Boolean, rows.GetFieldType(2));
        Assert.Equal(StoolapType.Float, rows.GetFieldType(3));
    }

    [Fact]
    public void Streaming_AllAccessors_OnSameRow()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (i INTEGER, f FLOAT, s TEXT, b BOOLEAN)");
        db.Execute("INSERT INTO t VALUES (?, ?, ?, ?)", 100L, 3.14, "hello", true);

        using var rows = db.QueryStream("SELECT i, f, s, b FROM t");
        Assert.True(rows.Read());
        Assert.Equal(100L, rows.GetInt64(0));
        Assert.Equal(100, rows.GetInt32(0));
        Assert.Equal(3.14, rows.GetDouble(1), 6);
        Assert.Equal(3.14f, rows.GetFloat(1), 4);
        Assert.Equal("hello", rows.GetString(2));
        Assert.True(rows.GetBoolean(3));
        Assert.False(rows.IsDBNull(0));
    }

    [Fact]
    public void Streaming_GetValue_BoxesCorrectly()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (a INTEGER, b TEXT, c BOOLEAN)");
        db.Execute("INSERT INTO t VALUES (1, 'x', true)");

        using var rows = db.QueryStream("SELECT a, b, c FROM t");
        Assert.True(rows.Read());
        Assert.Equal(1L, rows.GetValue(0));
        Assert.Equal("x", rows.GetValue(1));
        Assert.Equal(true, rows.GetValue(2));
    }

    [Fact]
    public void Streaming_NullColumn_GetValueReturnsNull()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (a INTEGER, b TEXT)");
        db.Execute("INSERT INTO t VALUES (?, ?)", 1, null);

        using var rows = db.QueryStream("SELECT a, b FROM t");
        Assert.True(rows.Read());
        Assert.Null(rows.GetValue(1));
        Assert.True(rows.IsDBNull(1));
    }

    [Fact]
    public void Streaming_ColumnNames_Available()
    {
        using var db = Database.OpenInMemory();
        db.Execute("CREATE TABLE t (foo INTEGER, bar TEXT)");
        db.Execute("INSERT INTO t VALUES (1, 'x')");

        using var rows = db.QueryStream("SELECT foo, bar FROM t");
        Assert.Equal(2, rows.ColumnCount);
        Assert.Equal("foo", rows.Columns[0]);
        Assert.Equal("bar", rows.Columns[1]);
    }
}
