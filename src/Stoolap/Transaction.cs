// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Runtime.InteropServices;
using Stoolap.Native;

namespace Stoolap;

/// <summary>
/// A transaction. Created by <see cref="Database.Begin()"/>. Must be ended
/// with either <see cref="Commit"/> or <see cref="Rollback"/>; disposing
/// without ending implicitly rolls back.
/// </summary>
public sealed class Transaction : IDisposable
{
    private readonly StoolapTxHandle _handle;
    private bool _ended;

    internal Transaction(StoolapTxHandle handle)
    {
        _handle = handle;
    }

    public long Execute(string sql)
    {
        ThrowIfEnded();
        ArgumentNullException.ThrowIfNull(sql);
        int rc = NativeMethods.stoolap_tx_exec(_handle.DangerousGetHandle(), sql, out var rowsAffected);
        if (rc != StatusCodes.Ok)
        {
            throw StoolapException.FromTx(_handle.DangerousGetHandle());
        }
        return rowsAffected;
    }

    public long Execute(string sql, params object?[] parameters)
    {
        ThrowIfEnded();
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);
        var binder = default(ParameterBinder);
        try
        {
            unsafe
            {
                Span<StoolapValue> values = parameters.Length <= 16
                    ? stackalloc StoolapValue[parameters.Length]
                    : new StoolapValue[parameters.Length];
                byte* scratch = stackalloc byte[ParameterBinder.RecommendedScratchSize];
                fixed (StoolapValue* ptr = values)
                {
                    binder.Bind(parameters, ptr, scratch, ParameterBinder.RecommendedScratchSize);
                    int rc = NativeMethods.stoolap_tx_exec_params(
                        _handle.DangerousGetHandle(), sql, ptr, parameters.Length, out var rowsAffected);
                    if (rc != StatusCodes.Ok)
                    {
                        throw StoolapException.FromTx(_handle.DangerousGetHandle());
                    }
                    return rowsAffected;
                }
            }
        }
        finally
        {
            binder.Dispose();
        }
    }

    public QueryResult Query(string sql)
    {
        ThrowIfEnded();
        ArgumentNullException.ThrowIfNull(sql);
        int rc = NativeMethods.stoolap_tx_query(_handle.DangerousGetHandle(), sql, out var rawRows);
        if (rc != StatusCodes.Ok || rawRows == 0)
        {
            throw StoolapException.FromTx(_handle.DangerousGetHandle());
        }
        return FetchAllAndClose(rawRows);
    }

    public QueryResult Query(string sql, params object?[] parameters)
    {
        ThrowIfEnded();
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);
        var binder = default(ParameterBinder);
        try
        {
            nint rawRows;
            unsafe
            {
                Span<StoolapValue> values = parameters.Length <= 16
                    ? stackalloc StoolapValue[parameters.Length]
                    : new StoolapValue[parameters.Length];
                byte* scratch = stackalloc byte[ParameterBinder.RecommendedScratchSize];
                fixed (StoolapValue* ptr = values)
                {
                    binder.Bind(parameters, ptr, scratch, ParameterBinder.RecommendedScratchSize);
                    int rc = NativeMethods.stoolap_tx_query_params(
                        _handle.DangerousGetHandle(), sql, ptr, parameters.Length, out rawRows);
                    if (rc != StatusCodes.Ok || rawRows == 0)
                    {
                        throw StoolapException.FromTx(_handle.DangerousGetHandle());
                    }
                }
            }
            return FetchAllAndClose(rawRows);
        }
        finally
        {
            binder.Dispose();
        }
    }

    /// <summary>Streaming version of <see cref="Query(string)"/>. Returns a
    /// row-by-row reader instead of materializing the full result set.</summary>
    public Rows QueryStream(string sql)
    {
        ThrowIfEnded();
        ArgumentNullException.ThrowIfNull(sql);
        int rc = NativeMethods.stoolap_tx_query(_handle.DangerousGetHandle(), sql, out var rawRows);
        if (rc != StatusCodes.Ok || rawRows == 0)
        {
            throw StoolapException.FromTx(_handle.DangerousGetHandle());
        }
        var handle = new StoolapRowsHandle();
        Marshal.InitHandle(handle, rawRows);
        return new Rows(handle);
    }

    /// <summary>Streaming version of <see cref="Query(string, object?[])"/>.</summary>
    public Rows QueryStream(string sql, params object?[] parameters)
    {
        ThrowIfEnded();
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);
        var binder = default(ParameterBinder);
        try
        {
            nint rawRows;
            unsafe
            {
                Span<StoolapValue> values = parameters.Length <= 16
                    ? stackalloc StoolapValue[parameters.Length]
                    : new StoolapValue[parameters.Length];
                byte* scratch = stackalloc byte[ParameterBinder.RecommendedScratchSize];
                fixed (StoolapValue* ptr = values)
                {
                    binder.Bind(parameters, ptr, scratch, ParameterBinder.RecommendedScratchSize);
                    int rc = NativeMethods.stoolap_tx_query_params(
                        _handle.DangerousGetHandle(), sql, ptr, parameters.Length, out rawRows);
                    if (rc != StatusCodes.Ok || rawRows == 0)
                    {
                        throw StoolapException.FromTx(_handle.DangerousGetHandle());
                    }
                }
            }
            var handle = new StoolapRowsHandle();
            Marshal.InitHandle(handle, rawRows);
            return new Rows(handle);
        }
        finally
        {
            binder.Dispose();
        }
    }

    /// <summary>Executes a prepared statement within this transaction.</summary>
    public long Execute(PreparedStatement statement, params object?[] parameters)
    {
        ThrowIfEnded();
        ArgumentNullException.ThrowIfNull(statement);
        ArgumentNullException.ThrowIfNull(parameters);
        var binder = default(ParameterBinder);
        try
        {
            unsafe
            {
                Span<StoolapValue> values = parameters.Length <= 16
                    ? stackalloc StoolapValue[parameters.Length]
                    : new StoolapValue[parameters.Length];
                byte* scratch = stackalloc byte[ParameterBinder.RecommendedScratchSize];
                fixed (StoolapValue* ptr = values)
                {
                    binder.Bind(parameters, ptr, scratch, ParameterBinder.RecommendedScratchSize);
                    int rc = NativeMethods.stoolap_tx_stmt_exec(
                        _handle.DangerousGetHandle(),
                        statement.DangerousHandle,
                        ptr,
                        parameters.Length,
                        out var rowsAffected);
                    if (rc != StatusCodes.Ok)
                    {
                        throw StoolapException.FromTx(_handle.DangerousGetHandle());
                    }
                    return rowsAffected;
                }
            }
        }
        finally
        {
            binder.Dispose();
        }
    }

    /// <summary>Queries a prepared statement within this transaction.</summary>
    public QueryResult Query(PreparedStatement statement, params object?[] parameters)
    {
        ThrowIfEnded();
        ArgumentNullException.ThrowIfNull(statement);
        ArgumentNullException.ThrowIfNull(parameters);
        var binder = default(ParameterBinder);
        try
        {
            nint rawRows;
            unsafe
            {
                Span<StoolapValue> values = parameters.Length <= 16
                    ? stackalloc StoolapValue[parameters.Length]
                    : new StoolapValue[parameters.Length];
                byte* scratch = stackalloc byte[ParameterBinder.RecommendedScratchSize];
                fixed (StoolapValue* ptr = values)
                {
                    binder.Bind(parameters, ptr, scratch, ParameterBinder.RecommendedScratchSize);
                    int rc = NativeMethods.stoolap_tx_stmt_query(
                        _handle.DangerousGetHandle(),
                        statement.DangerousHandle,
                        ptr,
                        parameters.Length,
                        out rawRows);
                    if (rc != StatusCodes.Ok || rawRows == 0)
                    {
                        throw StoolapException.FromTx(_handle.DangerousGetHandle());
                    }
                }
            }
            return FetchAllAndClose(rawRows);
        }
        finally
        {
            binder.Dispose();
        }
    }

    public void Commit()
    {
        ThrowIfEnded();
        // The native call consumes the handle: detach so the SafeHandle does
        // not try to free it again.
        var raw = _handle.Detach();
        _ended = true;
        int rc = NativeMethods.stoolap_tx_commit(raw);
        if (rc != StatusCodes.Ok)
        {
            // Pull the global error string since the handle is gone.
            var msg = StoolapException.ReadCString(NativeMethods.stoolap_errmsg(0))
                      ?? "stoolap_tx_commit failed";
            throw new StoolapException(msg, rc);
        }
    }

    public void Rollback()
    {
        if (_ended)
        {
            return;
        }
        var raw = _handle.Detach();
        _ended = true;
        int rc = NativeMethods.stoolap_tx_rollback(raw);
        if (rc != StatusCodes.Ok)
        {
            var msg = StoolapException.ReadCString(NativeMethods.stoolap_errmsg(0))
                      ?? "stoolap_tx_rollback failed";
            throw new StoolapException(msg, rc);
        }
    }

    public void Dispose()
    {
        if (_ended)
        {
            return;
        }
        // Implicit rollback on dispose. The SafeHandle's ReleaseHandle calls
        // stoolap_tx_rollback for us; just dispose it.
        _ended = true;
        _handle.Dispose();
    }

    private static QueryResult FetchAllAndClose(nint rawRows)
    {
        int rc = NativeMethods.stoolap_rows_fetch_all(rawRows, out var buf, out var len);
        try
        {
            if (rc != StatusCodes.Ok || buf == 0)
            {
                throw StoolapException.FromRows(rawRows);
            }
            unsafe
            {
                var span = new ReadOnlySpan<byte>((void*)buf, checked((int)len));
                var decoded = BinaryRowParser.Parse(span);
                return new QueryResult(decoded.Columns, decoded.Rows);
            }
        }
        finally
        {
            if (buf != 0)
            {
                NativeMethods.stoolap_buffer_free(buf, len);
            }
            NativeMethods.stoolap_rows_close(rawRows);
        }
    }

    private void ThrowIfEnded()
    {
        if (_ended)
        {
            throw new InvalidOperationException("Transaction has already been committed or rolled back.");
        }
    }
}
