// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Data;
using System.Data.Common;
using Stoolap.Native;

namespace Stoolap.Ado;

/// <summary>
/// ADO.NET connection wrapping a <see cref="Stoolap.Database"/>. Plug into
/// Dapper or any framework that speaks <see cref="DbConnection"/>.
/// </summary>
public sealed class StoolapConnection : DbConnection
{
    private Database? _database;
    private string _connectionString;
    private ConnectionState _state = ConnectionState.Closed;

    public StoolapConnection() : this(string.Empty) { }

    public StoolapConnection(string connectionString)
    {
        _connectionString = connectionString ?? string.Empty;
    }

    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set
        {
            if (_state != ConnectionState.Closed)
            {
                throw new InvalidOperationException("Cannot change ConnectionString while the connection is open.");
            }
            _connectionString = value ?? string.Empty;
        }
    }

    public override string Database => string.Empty;

    public override string DataSource
    {
        get
        {
            var builder = new StoolapConnectionStringBuilder { ConnectionString = _connectionString };
            return builder.DataSource;
        }
    }

    public override string ServerVersion => Stoolap.Database.Version;

    public override ConnectionState State => _state;

    /// <summary>The underlying high-level <see cref="Stoolap.Database"/>. Null when closed.</summary>
    internal Database? Inner => _database;

    public override void Open()
    {
        if (_state == ConnectionState.Open)
        {
            return;
        }

        var builder = new StoolapConnectionStringBuilder { ConnectionString = _connectionString };
        var dsn = builder.DataSource;
        if (string.IsNullOrEmpty(dsn))
        {
            dsn = "memory://";
        }

        _database = Stoolap.Database.Open(dsn);
        _state = ConnectionState.Open;
    }

    public override void Close()
    {
        if (_state == ConnectionState.Closed)
        {
            return;
        }
        _database?.Dispose();
        _database = null;
        _state = ConnectionState.Closed;
    }

    public override void ChangeDatabase(string databaseName)
        => throw new NotSupportedException("Stoolap does not support multiple catalogs per connection.");

    protected override DbCommand CreateDbCommand() => new StoolapCommand { Connection = this };

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        EnsureOpen();
        var native = isolationLevel switch
        {
            IsolationLevel.Snapshot => StoolapIsolationLevel.Snapshot,
            IsolationLevel.Unspecified or IsolationLevel.ReadCommitted => StoolapIsolationLevel.ReadCommitted,
            _ => throw new NotSupportedException($"Isolation level {isolationLevel} is not supported by stoolap."),
        };
        var tx = _database!.Begin(native);
        return new StoolapTransaction(this, tx, isolationLevel);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Close();
        }
        base.Dispose(disposing);
    }

    internal void EnsureOpen()
    {
        if (_state != ConnectionState.Open || _database is null)
        {
            throw new InvalidOperationException("Connection is not open.");
        }
    }
}
