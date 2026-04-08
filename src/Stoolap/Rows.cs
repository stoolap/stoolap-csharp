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
/// Streaming row reader. Wraps a <c>StoolapRows*</c> handle and exposes
/// per-cell accessors backed directly by the FFI getters. Implements
/// <see cref="IDisposable"/>; <c>Dispose()</c> closes the underlying handle.
/// </summary>
public sealed class Rows : IDisposable
{
    private readonly StoolapRowsHandle _handle;
    private string[]? _columnCache;
    private bool _hasCurrent;
    private bool _disposed;

    internal Rows(StoolapRowsHandle handle)
    {
        _handle = handle;
    }

    /// <summary>The number of columns in the result set.</summary>
    public int ColumnCount
    {
        get
        {
            ThrowIfDisposed();
            return NativeMethods.stoolap_rows_column_count(_handle.DangerousGetHandle());
        }
    }

    /// <summary>The column names. Cached after the first read.</summary>
    public IReadOnlyList<string> Columns
    {
        get
        {
            ThrowIfDisposed();
            if (_columnCache is null)
            {
                int count = ColumnCount;
                var arr = new string[count];
                var raw = _handle.DangerousGetHandle();
                for (int i = 0; i < count; i++)
                {
                    var ptr = NativeMethods.stoolap_rows_column_name(raw, i);
                    arr[i] = StoolapException.ReadCString(ptr) ?? string.Empty;
                }
                _columnCache = arr;
            }
            return _columnCache;
        }
    }

    /// <summary>
    /// Number of rows affected (DML results). Always 0 for SELECT.
    /// </summary>
    public long RowsAffected
    {
        get
        {
            ThrowIfDisposed();
            return NativeMethods.stoolap_rows_affected(_handle.DangerousGetHandle());
        }
    }

    /// <summary>
    /// Advance to the next row. Returns true if a row is available, false at end.
    /// </summary>
    public bool Read()
    {
        ThrowIfDisposed();
        var raw = _handle.DangerousGetHandle();
        int rc = NativeMethods.stoolap_rows_next(raw);
        switch (rc)
        {
            case StatusCodes.Row:
                _hasCurrent = true;
                return true;
            case StatusCodes.Done:
                _hasCurrent = false;
                return false;
            default:
                throw StoolapException.FromRows(raw);
        }
    }

    /// <summary>Returns the type of the value at <paramref name="index"/> in the current row.</summary>
    public StoolapType GetFieldType(int index)
    {
        EnsureCurrent();
        return (StoolapType)NativeMethods.stoolap_rows_column_type(_handle.DangerousGetHandle(), index);
    }

    /// <summary>True if the column at <paramref name="index"/> is NULL in the current row.</summary>
    public bool IsDBNull(int index)
    {
        EnsureCurrent();
        return NativeMethods.stoolap_rows_column_is_null(_handle.DangerousGetHandle(), index) != 0;
    }

    public long GetInt64(int index)
    {
        EnsureCurrent();
        return NativeMethods.stoolap_rows_column_int64(_handle.DangerousGetHandle(), index);
    }

    public int GetInt32(int index) => checked((int)GetInt64(index));

    public double GetDouble(int index)
    {
        EnsureCurrent();
        return NativeMethods.stoolap_rows_column_double(_handle.DangerousGetHandle(), index);
    }

    public float GetFloat(int index) => (float)GetDouble(index);

    public bool GetBoolean(int index)
    {
        EnsureCurrent();
        return NativeMethods.stoolap_rows_column_bool(_handle.DangerousGetHandle(), index) != 0;
    }

    public string? GetString(int index)
    {
        EnsureCurrent();
        var ptr = NativeMethods.stoolap_rows_column_text(_handle.DangerousGetHandle(), index, out var len);
        if (ptr == 0)
        {
            return null;
        }
        unsafe
        {
            return new string((sbyte*)ptr, 0, (int)len, System.Text.Encoding.UTF8);
        }
    }

    public DateTime GetDateTime(int index)
    {
        EnsureCurrent();
        long nanos = NativeMethods.stoolap_rows_column_timestamp(_handle.DangerousGetHandle(), index);
        return DateTime.UnixEpoch.AddTicks(nanos / 100L);
    }

    /// <summary>
    /// Returns the raw vector payload (packed f32 little-endian) as a copy.
    /// Returns an empty array for non-blob columns.
    /// </summary>
    public float[] GetVector(int index)
    {
        EnsureCurrent();
        var ptr = NativeMethods.stoolap_rows_column_blob(_handle.DangerousGetHandle(), index, out var len);
        if (ptr == 0 || len == 0)
        {
            return Array.Empty<float>();
        }
        int floatCount = (int)(len / sizeof(float));
        var result = new float[floatCount];
        unsafe
        {
            new ReadOnlySpan<float>((void*)ptr, floatCount).CopyTo(result);
        }
        return result;
    }

    /// <summary>Boxes the column value into a managed object.</summary>
    public object? GetValue(int index)
    {
        if (IsDBNull(index))
        {
            return null;
        }
        return GetFieldType(index) switch
        {
            StoolapType.Null => null,
            StoolapType.Integer => GetInt64(index),
            StoolapType.Float => GetDouble(index),
            StoolapType.Text => GetString(index),
            StoolapType.Json => GetString(index),
            StoolapType.Boolean => GetBoolean(index),
            StoolapType.Timestamp => GetDateTime(index),
            StoolapType.Blob => GetVector(index),
            _ => null,
        };
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

    private void EnsureCurrent()
    {
        ThrowIfDisposed();
        if (!_hasCurrent)
        {
            throw new InvalidOperationException("No current row. Call Read() first.");
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(Rows));
        }
    }
}
