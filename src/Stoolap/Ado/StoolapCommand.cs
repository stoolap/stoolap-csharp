// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Stoolap.Ado;

/// <summary>ADO.NET command for stoolap.</summary>
public sealed class StoolapCommand : DbCommand
{
    private StoolapConnection? _connection;
    private StoolapTransaction? _transaction;
    private readonly StoolapParameterCollection _parameters = new();

    public StoolapCommand() { }

    public StoolapCommand(string sql) { CommandText = sql; }

    public StoolapCommand(string sql, StoolapConnection connection)
    {
        CommandText = sql;
        _connection = connection;
    }

    [AllowNull]
    public override string CommandText { get; set; } = string.Empty;

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
            {
                throw new NotSupportedException("Only CommandType.Text is supported.");
            }
        }
    }

    protected override DbConnection? DbConnection
    {
        get => _connection;
        set => _connection = (StoolapConnection?)value;
    }

    protected override DbParameterCollection DbParameterCollection => _parameters;

    protected override DbTransaction? DbTransaction
    {
        get => _transaction;
        set
        {
            var tx = (StoolapTransaction?)value;
            if (tx is not null && _connection is not null && !ReferenceEquals(tx.Connection, _connection))
            {
                throw new InvalidOperationException(
                    "Transaction was started by a different connection than this command uses.");
            }
            _transaction = tx;
        }
    }

    public override bool DesignTimeVisible { get; set; } = true;

    public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.None;

    public override void Cancel() { /* no-op */ }

    public override void Prepare() { /* prepared on each Execute; could be cached later */ }

    protected override DbParameter CreateDbParameter() => new StoolapParameter();

    public override int ExecuteNonQuery()
    {
        EnsureConnection();
        var (sql, args) = BuildPositional();
        long affected;
        if (_transaction is not null)
        {
            EnsureTransactionOwnedByConnection();
            affected = args.Length == 0
                ? _transaction.Inner.Execute(sql)
                : _transaction.Inner.Execute(sql, args);
        }
        else
        {
            affected = args.Length == 0
                ? _connection!.Inner!.Execute(sql)
                : _connection!.Inner!.Execute(sql, args);
        }
        return checked((int)affected);
    }

    public override object? ExecuteScalar()
    {
        EnsureConnection();
        var (sql, args) = BuildPositional();
        QueryResult result;
        if (_transaction is not null)
        {
            EnsureTransactionOwnedByConnection();
            result = args.Length == 0
                ? _transaction.Inner.Query(sql)
                : _transaction.Inner.Query(sql, args);
        }
        else
        {
            result = args.Length == 0
                ? _connection!.Inner!.Query(sql)
                : _connection!.Inner!.Query(sql, args);
        }
        if (result.RowCount == 0 || result.ColumnCount == 0)
        {
            return null;
        }
        return result[0, 0] ?? DBNull.Value;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        EnsureConnection();
        var (sql, args) = BuildPositional();
        Rows rows;
        if (_transaction is not null)
        {
            EnsureTransactionOwnedByConnection();
            rows = args.Length == 0
                ? _transaction.Inner.QueryStream(sql)
                : _transaction.Inner.QueryStream(sql, args);
        }
        else
        {
            rows = args.Length == 0
                ? _connection!.Inner!.QueryStream(sql)
                : _connection!.Inner!.QueryStream(sql, args);
        }
        return new StoolapDataReader(rows);
    }

    private (string sql, object?[] args) BuildPositional()
    {
        if (string.IsNullOrEmpty(CommandText))
        {
            throw new InvalidOperationException("CommandText is required.");
        }

        if (_parameters.Count == 0)
        {
            return (CommandText, Array.Empty<object?>());
        }

        var rewrite = NamedParameterRewriter.Rewrite(CommandText);
        if (rewrite.ParameterNames.Count == 0)
        {
            // SQL had no named placeholders, but parameters were added; treat
            // them as positional in declaration order.
            var positional = new object?[_parameters.Count];
            for (int i = 0; i < _parameters.Count; i++)
            {
                positional[i] = ((StoolapParameter)_parameters[i]).Value;
            }
            return (CommandText, positional);
        }

        var args = new object?[rewrite.ParameterNames.Count];
        for (int i = 0; i < rewrite.ParameterNames.Count; i++)
        {
            int idx = _parameters.IndexOf(rewrite.ParameterNames[i]);
            if (idx < 0)
            {
                throw new InvalidOperationException(
                    $"SQL references parameter '{rewrite.ParameterNames[i]}' but no matching parameter was supplied.");
            }
            args[i] = ((StoolapParameter)_parameters[idx]).Value;
        }
        return (rewrite.Sql, args);
    }

    private void EnsureConnection()
    {
        if (_connection is null)
        {
            throw new InvalidOperationException("Command has no connection.");
        }
        _connection.EnsureOpen();
    }

    private void EnsureTransactionOwnedByConnection()
    {
        if (_transaction is not null && !ReferenceEquals(_transaction.Connection, _connection))
        {
            throw new InvalidOperationException(
                "Transaction was started by a different connection than this command uses.");
        }
    }
}
