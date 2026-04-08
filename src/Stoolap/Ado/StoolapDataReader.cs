// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Collections;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Stoolap.Ado;

/// <summary>
/// ADO.NET data reader for stoolap. Wraps a single <see cref="Rows"/> streaming
/// handle (the same object for both connection- and transaction-scoped queries).
///
/// Constructor-time peek-ahead:
/// <list type="bullet">
///   <item><b><see cref="HasRows"/></b> reflects whether the underlying result
///   actually contains rows, instead of blindly returning <c>true</c>.</item>
///   <item><b><see cref="GetFieldType(int)"/></b> returns stable per-column CLR
///   types cached from the first row, so callers that inspect schema metadata
///   before reading (EF, Dapper, DbColumnSchemaGenerator) get correct
///   answers even on empty results or NULL-first rows.</item>
/// </list>
/// The peek consumes one row from the native cursor and stashes it; the first
/// user <see cref="Read"/> returns the cached row instead of advancing.
/// </summary>
public sealed class StoolapDataReader : DbDataReader
{
    private readonly Rows _rows;
    private readonly bool _hasInitialRow;
    /// <summary>
    /// Per-column native types snapshotted during the constructor peek. For
    /// empty results every slot is <see cref="Native.StoolapType.Null"/>,
    /// which <see cref="MapType"/> translates to <see cref="object"/>.
    /// Stored as the native enum (not <see cref="Type"/>) so the trim analyzer
    /// can route the returned value through <see cref="MapType"/>, which has
    /// the right <c>DynamicallyAccessedMembers</c> annotation on its return.
    /// </summary>
    private readonly Native.StoolapType[] _columnNativeTypes;
    private bool _started;
    private bool _currentRowValid;
    private bool _closed;

    internal StoolapDataReader(Rows rows)
    {
        _rows = rows;

        // Peek one row so HasRows and GetFieldType can report stable metadata
        // without forcing the caller to Read() first. If the result set is
        // empty, we still have rows.ColumnCount / rows.Columns available.
        _hasInitialRow = _rows.Read();

        int colCount = _rows.ColumnCount;
        _columnNativeTypes = new Native.StoolapType[colCount];
        if (_hasInitialRow)
        {
            for (int i = 0; i < colCount; i++)
            {
                _columnNativeTypes[i] = _rows.GetFieldType(i);
            }
        }
        // Else: array already zero-initialized to StoolapType.Null, which
        // MapType translates to typeof(object). Empty results report object
        // for every column; callers that need precise types on empty results
        // can check HasRows first.
    }

    public override int Depth => 0;

    public override int FieldCount
    {
        get
        {
            ThrowIfClosed();
            return _rows.ColumnCount;
        }
    }

    public override bool HasRows
    {
        get
        {
            ThrowIfClosed();
            return _hasInitialRow;
        }
    }

    public override bool IsClosed => _closed;

    public override int RecordsAffected
    {
        get
        {
            if (_closed)
            {
                return -1;
            }
            return checked((int)_rows.RowsAffected);
        }
    }

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override bool Read()
    {
        ThrowIfClosed();

        // First call after construction: replay the peeked row.
        if (!_started)
        {
            _started = true;
            _currentRowValid = _hasInitialRow;
            return _hasInitialRow;
        }

        // Subsequent calls: advance the native cursor normally. The constructor
        // peek already positioned the cursor on the first row, and the first
        // user Read() replayed it without advancing, so every subsequent call
        // moves the cursor forward by one.
        _currentRowValid = _rows.Read();
        return _currentRowValid;
    }

    public override bool NextResult() => false;

    public override void Close()
    {
        if (_closed)
        {
            return;
        }
        _closed = true;
        _rows.Dispose();
    }

    public override string GetName(int ordinal)
    {
        ThrowIfClosed();
        return _rows.Columns[ordinal];
    }

    public override int GetOrdinal(string name)
    {
        ThrowIfClosed();
        var columns = _rows.Columns;
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i], name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int ordinal)
    {
        ThrowIfClosed();
        if ((uint)ordinal >= (uint)_columnNativeTypes.Length)
        {
            throw new IndexOutOfRangeException(nameof(ordinal));
        }
        return MapType(_columnNativeTypes[ordinal]);
    }

    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    public override bool IsDBNull(int ordinal)
    {
        ThrowIfClosed();
        EnsureCurrentRow();
        return _rows.IsDBNull(ordinal);
    }

    public override object GetValue(int ordinal)
    {
        ThrowIfClosed();
        EnsureCurrentRow();
        return _rows.GetValue(ordinal) ?? DBNull.Value;
    }

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }
        return count;
    }

    public override bool GetBoolean(int ordinal)
    {
        ThrowIfClosed();
        EnsureCurrentRow();
        return _rows.GetBoolean(ordinal);
    }

    public override byte GetByte(int ordinal) => Convert.ToByte(GetInt64(ordinal));

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException("GetBytes is not supported by stoolap; use GetValue() and read the float[] vector payload.");

    public override char GetChar(int ordinal)
    {
        var s = GetString(ordinal);
        return string.IsNullOrEmpty(s) ? '\0' : s[0];
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException("GetChars is not supported. Use GetString() instead.");

    public override DateTime GetDateTime(int ordinal)
    {
        ThrowIfClosed();
        EnsureCurrentRow();
        return _rows.GetDateTime(ordinal);
    }

    public override decimal GetDecimal(int ordinal) => (decimal)GetDouble(ordinal);

    public override double GetDouble(int ordinal)
    {
        ThrowIfClosed();
        EnsureCurrentRow();
        return _rows.GetDouble(ordinal);
    }

    public override float GetFloat(int ordinal) => (float)GetDouble(ordinal);

    public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));

    public override short GetInt16(int ordinal) => checked((short)GetInt64(ordinal));

    public override int GetInt32(int ordinal) => checked((int)GetInt64(ordinal));

    public override long GetInt64(int ordinal)
    {
        ThrowIfClosed();
        EnsureCurrentRow();
        return _rows.GetInt64(ordinal);
    }

    public override string GetString(int ordinal)
    {
        ThrowIfClosed();
        EnsureCurrentRow();
        return _rows.GetString(ordinal) ?? string.Empty;
    }

    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    private static Type MapType(Stoolap.Native.StoolapType t) => t switch
    {
        Native.StoolapType.Integer => typeof(long),
        Native.StoolapType.Float => typeof(double),
        Native.StoolapType.Text => typeof(string),
        Native.StoolapType.Json => typeof(string),
        Native.StoolapType.Boolean => typeof(bool),
        Native.StoolapType.Timestamp => typeof(DateTime),
        Native.StoolapType.Blob => typeof(float[]),
        _ => typeof(object),
    };

    private void EnsureCurrentRow()
    {
        if (!_currentRowValid)
        {
            throw new InvalidOperationException(
                "No current row. Call Read() before accessing column values.");
        }
    }

    private void ThrowIfClosed()
    {
        if (_closed)
        {
            throw new ObjectDisposedException(nameof(StoolapDataReader));
        }
    }
}
