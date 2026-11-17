// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using Stoolap.Ado;
using Xunit;

namespace Stoolap.Tests;

/// <summary>
/// Direct unit tests for <see cref="NamedParameterRewriter"/>. These verify
/// the lexer correctness for every supported sigil and every kind of token
/// the rewriter has to skip (literals, identifiers, comments).
/// </summary>
public class NamedParameterRewriterTests
{
    [Fact]
    public void NoParams_NoChange()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT 1");
        Assert.Equal("SELECT 1", r.Sql);
        Assert.Empty(r.ParameterNames);
    }

    [Fact]
    public void EmptySql_NoCrash()
    {
        var r = NamedParameterRewriter.Rewrite(string.Empty);
        Assert.Equal(string.Empty, r.Sql);
        Assert.Empty(r.ParameterNames);
    }

    [Fact]
    public void SingleAtSigil_Rewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT * FROM t WHERE id = @id");
        Assert.Equal("SELECT * FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void SingleColonSigil_Rewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT * FROM t WHERE id = :id");
        Assert.Equal("SELECT * FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void SingleDollarSigil_Rewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT * FROM t WHERE id = $id");
        Assert.Equal("SELECT * FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void MixedSigils_AllRewritten_OrderPreserved()
    {
        var r = NamedParameterRewriter.Rewrite("INSERT INTO t VALUES (@a, :b, $c)");
        Assert.Equal("INSERT INTO t VALUES ($1, $2, $3)", r.Sql);
        Assert.Equal(new[] { "a", "b", "c" }, r.ParameterNames);
    }

    [Fact]
    public void DuplicateParameterName_SharesOneSlot()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT * FROM t WHERE a = @x OR b = @x");
        Assert.Equal("SELECT * FROM t WHERE a = $1 OR b = $1", r.Sql);
        Assert.Equal(new[] { "x" }, r.ParameterNames);
    }

    [Fact]
    public void SigilInsideStringLiteral_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT 'hello @world' FROM t WHERE id = @id");
        Assert.Equal("SELECT 'hello @world' FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void EscapedQuoteInsideLiteral_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT 'it''s @done' FROM t WHERE id = @id");
        Assert.Equal("SELECT 'it''s @done' FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void SigilInsideQuotedIdentifier_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT \"col@name\" FROM t WHERE id = @id");
        Assert.Equal("SELECT \"col@name\" FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void EscapedDoubleQuoteInsideIdentifier_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT \"a\"\"@b\" FROM t WHERE id = @id");
        Assert.Equal("SELECT \"a\"\"@b\" FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void SigilInsideLineComment_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT 1 -- comment with @ignored\nFROM t WHERE id = @id");
        Assert.Contains("$1", r.Sql);
        Assert.DoesNotContain("@id", r.Sql);
        Assert.Contains("@ignored", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void SigilInsideBlockComment_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT 1 /* @ignored */ FROM t WHERE id = @id");
        Assert.Contains("@ignored", r.Sql);
        Assert.DoesNotContain("@id", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void EmailAddressInLiteral_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("INSERT INTO t VALUES ('alice@example.com', @id)");
        Assert.Equal("INSERT INTO t VALUES ('alice@example.com', $1)", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void ParameterNameWithUnderscores_AllowedInIdentifier()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT * FROM t WHERE id = @user_id_value");
        Assert.Equal("SELECT * FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "user_id_value" }, r.ParameterNames);
    }

    [Fact]
    public void ParameterNameWithDigits_AllowedAfterFirstChar()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT @p1, @p2, @p3");
        Assert.Equal("SELECT $1, $2, $3", r.Sql);
        Assert.Equal(new[] { "p1", "p2", "p3" }, r.ParameterNames);
    }

    [Fact]
    public void OnlySigil_WithNoIdentifier_LeftAlone()
    {
        // A bare '@' or ':' that isn't followed by an identifier is NOT a
        // parameter and must be left in the SQL untouched.
        var r = NamedParameterRewriter.Rewrite("SELECT col @ FROM t");
        Assert.Equal("SELECT col @ FROM t", r.Sql);
        Assert.Empty(r.ParameterNames);
    }

    [Fact]
    public void DigitAfterSigil_NotTreatedAsParam()
    {
        // Identifiers cannot start with a digit; "@1" should not be rewritten.
        var r = NamedParameterRewriter.Rewrite("SELECT @1col FROM t");
        Assert.Equal("SELECT @1col FROM t", r.Sql);
        Assert.Empty(r.ParameterNames);
    }

    [Fact]
    public void SigilInsideBacktickIdentifier_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT `a@b`, `x``@y` FROM t WHERE id = @id");
        Assert.Equal("SELECT `a@b`, `x``@y` FROM t WHERE id = $1", r.Sql);
        Assert.Equal(new[] { "id" }, r.ParameterNames);
    }

    [Fact]
    public void DoubleColonCast_NotRewritten()
    {
        var r = NamedParameterRewriter.Rewrite("SELECT x::INTEGER, :y::TEXT FROM t");
        Assert.Equal("SELECT x::INTEGER, $1::TEXT FROM t", r.Sql);
        Assert.Equal(new[] { "y" }, r.ParameterNames);
    }

    [Theory]
    [InlineData("SELECT @a + ?", true)]
    [InlineData("SELECT $1, @a", true)]
    [InlineData("SELECT @a, '?', '$1' -- ?\n /* $2 */", false)]
    [InlineData("SELECT @a, \"q?\", `$1`", false)]
    public void PositionalPlaceholders_AreDetected(string sql, bool expected)
    {
        Assert.Equal(expected, NamedParameterRewriter.Rewrite(sql).HasPositional);
    }
}
