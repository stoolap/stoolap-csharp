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

    // Ended by Close(), so nothing keeps running on a closed connection.
    private readonly List<StoolapTransaction> _transactions = new();
    private readonly List<StoolapDataReader> _readers = new();

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

    /// <summary>
    /// Closes the connection. Open data readers are closed and transactions
    /// that have not ended are rolled back first, as ADO.NET requires.
    /// </summary>
    public override void Close()
    {
        if (_state == ConnectionState.Closed)
        {
            return;
        }
        // Mark closed first: a reader opened with CommandBehavior.CloseConnection
        // calls back into Close() and must find nothing left to do.
        _state = ConnectionState.Closed;

        var readers = _readers.ToArray();
        _readers.Clear();
        foreach (var reader in readers)
        {
            reader.CloseFromConnection();
        }

        var transactions = _transactions.ToArray();
        _transactions.Clear();
        foreach (var tx in transactions)
        {
            tx.Dispose(); // rolls back if still open
        }

        _database?.Dispose();
        _database = null;
    }

    internal void RegisterReader(StoolapDataReader reader) => _readers.Add(reader);

    internal void OnReaderClosed(StoolapDataReader reader, bool closeConnection)
    {
        _readers.Remove(reader);
        if (closeConnection)
        {
            Close();
        }
    }

    internal void OnTransactionEnded(StoolapTransaction tx) => _transactions.Remove(tx);

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
        var tx = new StoolapTransaction(this, _database!.Begin(native), isolationLevel);
        _transactions.Add(tx);
        return tx;
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
