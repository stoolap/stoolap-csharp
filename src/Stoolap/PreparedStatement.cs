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
    private readonly ColumnHeaderCache _columns = new();
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
        ArgumentNullException.ThrowIfNull(parameters);
        return QueryAll(0, parameters);
    }

    /// <summary>
    /// Runs the statement through <c>stoolap_stmt_query_all</c>, inside
    /// <paramref name="tx"/> when it is not 0. One P/Invoke call, no rows
    /// handle; the packed buffer belongs to the statement and is decoded
    /// before returning.
    /// </summary>
    [SkipLocalsInit]
    internal QueryResult QueryAll(nint tx, ReadOnlySpan<object?> parameters)
    {
        ThrowIfDisposed();
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
                    var stmt = _handle.DangerousGetHandle();
                    int rc = NativeMethods.stoolap_stmt_query_all(
                        stmt, tx, ptr, parameters.Length, out var buf, out var len);
                    if (rc != StatusCodes.Ok || buf == 0)
                    {
                        throw StoolapException.FromStmt(stmt);
                    }
                    return BinaryRowParser.Parse(
                        new ReadOnlySpan<byte>((void*)buf, checked((int)len)), _columns);
                }
            }
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

    /// <summary>The native handle; throws once the statement is disposed, so a
    /// freed pointer never reaches the engine.</summary>
    internal nint DangerousHandle
    {
        get
        {
            ThrowIfDisposed();
            return _handle.DangerousGetHandle();
        }
    }

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
}
