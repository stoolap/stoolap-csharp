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

public class ConnectionStringBuilderTests
{
    [Fact]
    public void DataSource_DefaultsToEmpty()
    {
        var b = new StoolapConnectionStringBuilder();
        Assert.Equal(string.Empty, b.DataSource);
    }

    [Fact]
    public void DataSource_RoundTripThroughConnectionString()
    {
        var b = new StoolapConnectionStringBuilder { DataSource = "memory://" };
        var clone = new StoolapConnectionStringBuilder { ConnectionString = b.ConnectionString };
        Assert.Equal("memory://", clone.DataSource);
    }

    [Fact]
    public void DataSource_NormalizedFromDataSourceKeyword()
    {
        var b = new StoolapConnectionStringBuilder { ConnectionString = "DataSource=memory://" };
        Assert.Equal("memory://", b.DataSource);
    }

    [Fact]
    public void DataSource_NormalizedFromDsnKeyword()
    {
        var b = new StoolapConnectionStringBuilder { ConnectionString = "DSN=memory://" };
        Assert.Equal("memory://", b.DataSource);
    }

    [Fact]
    public void IndexerSetter_CanonicalKey_ReadBack()
    {
        var b = new StoolapConnectionStringBuilder();
        b["Data Source"] = "file:///tmp/db";
        Assert.Equal("file:///tmp/db", b.DataSource);
    }

    [Fact]
    public void IndexerSetter_AltSpelling_NormalizedToCanonical()
    {
        var b = new StoolapConnectionStringBuilder();
        b["DataSource"] = "memory://";
        Assert.Equal("memory://", b.DataSource);
    }

    [Fact]
    public void Connection_OpensFromDataSource()
    {
        var cs = new StoolapConnectionStringBuilder
        {
            DataSource = $"memory://test-{Guid.NewGuid():N}",
        }.ConnectionString;
        using var conn = new StoolapConnection(cs);
        conn.Open();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);
    }

    [Fact]
    public void Connection_DefaultsToInMemoryWhenEmpty()
    {
        // Note: empty DSN routes to "memory://" which is the shared registry
        // entry. Behavior we care about here is just that Open() succeeds.
        using var conn = new StoolapConnection();
        conn.Open();
        Assert.Equal(System.Data.ConnectionState.Open, conn.State);
    }
}
