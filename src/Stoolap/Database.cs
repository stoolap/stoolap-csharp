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
/// A connection to a stoolap database. Wraps a single <c>StoolapDB*</c>.
///
/// Threading: a <see cref="Database"/> instance owns one query executor and
/// is intended to be used from a single thread at a time. To use stoolap from
/// multiple threads, call <see cref="Clone"/> once per thread; clones share
/// the underlying engine but have independent executors and error state.
/// </summary>
public sealed class Database : IDisposable
{
    private readonly StoolapDbHandle _handle;
    private bool _disposed;

    private Database(StoolapDbHandle handle)
    {
        _handle = handle;
    }

    static Database()
    {
        NativeMethods.EnsureLoaded();
    }

    /// <summary>Returns the version string of the underlying libstoolap.</summary>
    public static string Version
    {
        get
        {
            NativeMethods.EnsureLoaded();
            var ptr = NativeMethods.stoolap_version();
            return StoolapException.ReadCString(ptr) ?? string.Empty;
        }
    }

    // ----- open / close -----

    /// <summary>
    /// Opens a database from a DSN. Use <c>"memory://"</c> for an in-memory
    /// database, or <c>"file:///absolute/path"</c> for a persistent one.
    /// </summary>
    public static Database Open(string dsn)
    {
        ArgumentNullException.ThrowIfNull(dsn);
        int rc = NativeMethods.stoolap_open(dsn, out var raw);
        if (rc != StatusCodes.Ok || raw == 0)
        {
            var msg = StoolapException.ReadCString(NativeMethods.stoolap_errmsg(0))
                      ?? "stoolap_open failed";
            throw new StoolapException(msg, rc);
        }
        var handle = new StoolapDbHandle();
        Marshal.InitHandle(handle, raw);
        return new Database(handle);
    }

    /// <summary>Opens a fresh in-memory database.</summary>
    public static Database OpenInMemory()
    {
        int rc = NativeMethods.stoolap_open_in_memory(out var raw);
        if (rc != StatusCodes.Ok || raw == 0)
        {
            var msg = StoolapException.ReadCString(NativeMethods.stoolap_errmsg(0))
                      ?? "stoolap_open_in_memory failed";
            throw new StoolapException(msg, rc);
        }
        var handle = new StoolapDbHandle();
        Marshal.InitHandle(handle, raw);
        return new Database(handle);
    }

    /// <summary>
    /// Returns a new <see cref="Database"/> handle backed by the same engine.
    /// Each clone is safe to use from a single thread; one clone per thread
    /// is the recommended pattern for parallel workloads.
    /// </summary>
    public Database Clone()
    {
        ThrowIfDisposed();
        int rc = NativeMethods.stoolap_clone(_handle.DangerousGetHandle(), out var raw);
        if (rc != StatusCodes.Ok || raw == 0)
        {
            throw StoolapException.FromDb(_handle.DangerousGetHandle());
        }
        var clone = new StoolapDbHandle();
        Marshal.InitHandle(clone, raw);
        return new Database(clone);
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

    // ----- exec / query -----

    /// <summary>
    /// Executes a non-query SQL statement (DDL or DML) and returns the
    /// number of rows affected.
    /// </summary>
    public long Execute(string sql)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(sql);
        int rc = NativeMethods.stoolap_exec(_handle.DangerousGetHandle(), sql, out var rowsAffected);
        if (rc != StatusCodes.Ok)
        {
            throw StoolapException.FromDb(_handle.DangerousGetHandle());
        }
        return rowsAffected;
    }

    /// <summary>
    /// Executes a non-query SQL statement with positional parameters.
    /// </summary>
    public long Execute(string sql, params object?[] parameters)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);
        return ExecuteCore(sql, parameters);
    }

    [SkipLocalsInit]
    private long ExecuteCore(string sql, ReadOnlySpan<object?> parameters)
    {
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
                    int rc = NativeMethods.stoolap_exec_params(
                        _handle.DangerousGetHandle(), sql, ptr, parameters.Length, out var rowsAffected);
                    if (rc != StatusCodes.Ok)
                    {
                        throw StoolapException.FromDb(_handle.DangerousGetHandle());
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

    /// <summary>
    /// Runs a query and returns a fully-materialized <see cref="QueryResult"/>.
    /// Uses the binary fetch-all path: one P/Invoke crossing per call, no
    /// per-cell marshalling.
    /// </summary>
    public QueryResult Query(string sql)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(sql);
        int rc = NativeMethods.stoolap_query(_handle.DangerousGetHandle(), sql, out var rawRows);
        if (rc != StatusCodes.Ok || rawRows == 0)
        {
            throw StoolapException.FromDb(_handle.DangerousGetHandle());
        }
        return FetchAllAndClose(rawRows);
    }

    /// <summary>Runs a parameterized query and returns a fully-materialized result.</summary>
    public QueryResult Query(string sql, params object?[] parameters)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(parameters);
        return QueryCore(sql, parameters);
    }

    private QueryResult QueryCore(string sql, ReadOnlySpan<object?> parameters)
    {
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
                    int rc = NativeMethods.stoolap_query_params(
                        _handle.DangerousGetHandle(), sql, ptr, parameters.Length, out rawRows);
                    if (rc != StatusCodes.Ok || rawRows == 0)
                    {
                        throw StoolapException.FromDb(_handle.DangerousGetHandle());
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

    /// <summary>
    /// Runs a query and returns a streaming <see cref="Rows"/> reader. Use
    /// this when results may be large and you want to read row-by-row without
    /// materializing the whole set.
    /// </summary>
    public Rows QueryStream(string sql)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(sql);
        int rc = NativeMethods.stoolap_query(_handle.DangerousGetHandle(), sql, out var rawRows);
        if (rc != StatusCodes.Ok || rawRows == 0)
        {
            throw StoolapException.FromDb(_handle.DangerousGetHandle());
        }
        var handle = new StoolapRowsHandle();
        Marshal.InitHandle(handle, rawRows);
        return new Rows(handle);
    }

    /// <summary>Runs a parameterized streaming query.</summary>
    public Rows QueryStream(string sql, params object?[] parameters)
    {
        ThrowIfDisposed();
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
                    int rc = NativeMethods.stoolap_query_params(
                        _handle.DangerousGetHandle(), sql, ptr, parameters.Length, out rawRows);
                    if (rc != StatusCodes.Ok || rawRows == 0)
                    {
                        throw StoolapException.FromDb(_handle.DangerousGetHandle());
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

    // ----- prepared statements -----

    /// <summary>Prepares a statement for repeated execution.</summary>
    public PreparedStatement Prepare(string sql)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(sql);
        int rc = NativeMethods.stoolap_prepare(_handle.DangerousGetHandle(), sql, out var rawStmt);
        if (rc != StatusCodes.Ok || rawStmt == 0)
        {
            throw StoolapException.FromDb(_handle.DangerousGetHandle());
        }
        var handle = new StoolapStmtHandle();
        Marshal.InitHandle(handle, rawStmt);
        return new PreparedStatement(handle);
    }

    // ----- transactions -----

    /// <summary>Begins a transaction with the default isolation level (READ COMMITTED).</summary>
    public Transaction Begin()
    {
        ThrowIfDisposed();
        int rc = NativeMethods.stoolap_begin(_handle.DangerousGetHandle(), out var rawTx);
        if (rc != StatusCodes.Ok || rawTx == 0)
        {
            throw StoolapException.FromDb(_handle.DangerousGetHandle());
        }
        var handle = new StoolapTxHandle();
        Marshal.InitHandle(handle, rawTx);
        return new Transaction(handle);
    }

    /// <summary>Begins a transaction with an explicit isolation level.</summary>
    public Transaction Begin(StoolapIsolationLevel isolation)
    {
        ThrowIfDisposed();
        int rc = NativeMethods.stoolap_begin_with_isolation(
            _handle.DangerousGetHandle(), (int)isolation, out var rawTx);
        if (rc != StatusCodes.Ok || rawTx == 0)
        {
            throw StoolapException.FromDb(_handle.DangerousGetHandle());
        }
        var handle = new StoolapTxHandle();
        Marshal.InitHandle(handle, rawTx);
        return new Transaction(handle);
    }

    // ----- internals -----

    internal nint DangerousHandle => _handle.DangerousGetHandle();

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

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(Database));
        }
    }
}
