// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Stoolap.Native;

namespace Stoolap;

/// <summary>
/// A prepared SQL statement. Created by <see cref="Database.Prepare"/>.
/// Reusable across executions; thread-confined like its parent connection.
/// </summary>
public sealed class PreparedStatement : IDisposable
{
    private readonly StoolapStmtHandle _handle;
    private bool _disposed;

    internal PreparedStatement(StoolapStmtHandle handle)
    {
        _handle = handle;
    }

    /// <summary>Returns the SQL text used to prepare this statement.</summary>
    public string Sql
    {
        get
        {
            ThrowIfDisposed();
            var ptr = NativeMethods.stoolap_stmt_sql(_handle.DangerousGetHandle());
            return StoolapException.ReadCString(ptr) ?? string.Empty;
        }
    }

    public long Execute() => Execute(Array.Empty<object?>());

    [SkipLocalsInit]
    public long Execute(params object?[] parameters)
    {
        ThrowIfDisposed();
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
                    int rc = NativeMethods.stoolap_stmt_exec(
                        _handle.DangerousGetHandle(), ptr, parameters.Length, out var rowsAffected);
                    if (rc != StatusCodes.Ok)
                    {
                        throw StoolapException.FromStmt(_handle.DangerousGetHandle());
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

    public QueryResult Query() => Query(Array.Empty<object?>());

    public QueryResult Query(params object?[] parameters)
    {
        ThrowIfDisposed();
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
                    int rc = NativeMethods.stoolap_stmt_query(
                        _handle.DangerousGetHandle(), ptr, parameters.Length, out rawRows);
                    if (rc != StatusCodes.Ok || rawRows == 0)
                    {
                        throw StoolapException.FromStmt(_handle.DangerousGetHandle());
                    }
                }
            }
            return Database_FetchAllAndClose(rawRows);
        }
        finally
        {
            binder.Dispose();
        }
    }

    /// <summary>Returns a streaming reader for repeated row-by-row consumption.</summary>
    public Rows QueryStream(params object?[] parameters)
    {
        ThrowIfDisposed();
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
                    int rc = NativeMethods.stoolap_stmt_query(
                        _handle.DangerousGetHandle(), ptr, parameters.Length, out rawRows);
                    if (rc != StatusCodes.Ok || rawRows == 0)
                    {
                        throw StoolapException.FromStmt(_handle.DangerousGetHandle());
                    }
                }
            }
            var rowsHandle = new StoolapRowsHandle();
            Marshal.InitHandle(rowsHandle, rawRows);
            return new Rows(rowsHandle);
        }
        finally
        {
            binder.Dispose();
        }
    }

    internal nint DangerousHandle => _handle.DangerousGetHandle();

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        _handle.Dispose();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(PreparedStatement));
        }
    }

    // Mirrors Database.FetchAllAndClose; duplicated locally to keep it private.
    private static QueryResult Database_FetchAllAndClose(nint rawRows)
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
}
