// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Stoolap.Native;

namespace Stoolap.Ado;

/// <summary>
/// ADO.NET data reader for stoolap. Wraps a single <see cref="Rows"/> streaming
/// handle (the same object for both connection- and transaction-scoped queries).
///
/// Column types: the engine reports a value's type, not a result column's
/// declared type, so <see cref="GetFieldType(int)"/> is learned from the data.
/// The constructor reads the first row, and when a column is NULL there it
/// reads ahead (up to <see cref="TypeLookAheadRows"/> rows, buffered and
/// replayed in order) until every column has shown a non-NULL value. Callers
/// that build a mapper from the schema before reading (Dapper,
/// DbColumnSchemaGenerator) then get the real types even when the first row
/// holds NULLs. A column that is NULL in every row read ahead, and every
/// column of an empty result, reports <see cref="object"/>.
/// </summary>
public sealed class StoolapDataReader : DbDataReader
{
    /// <summary>The most rows read ahead to learn column types.</summary>
    internal const int TypeLookAheadRows = 64;

    private readonly Rows _rows;
    private readonly StoolapConnection _connection;
    private readonly bool _closeConnection;
    private readonly StoolapType[] _columnNativeTypes;
    private readonly int _recordsAffected;
    private readonly bool _hasRows;

    // Rows read ahead for type discovery, replayed before the native cursor
    // resumes. Null when the first row already typed every column.
    private readonly Queue<object?[]>? _buffered;
    private object?[]? _bufferedCurrent;
    private bool _firstRowPending;
    private bool _nativeExhausted;
    private bool _currentRowValid;
    private bool _closed;

    internal StoolapDataReader(Rows rows, StoolapConnection connection, CommandBehavior behavior)
    {
        _rows = rows;
        _connection = connection;
        _closeConnection = (behavior & CommandBehavior.CloseConnection) != 0;

        int colCount = rows.ColumnCount;
        _columnNativeTypes = new StoolapType[colCount];
        // ADO.NET convention: -1 for a statement that returns rows.
        _recordsAffected = colCount > 0 ? -1 : checked((int)rows.RowsAffected);

        if (!rows.Read())
        {
            _nativeExhausted = true;
            return;
        }
        _hasRows = true;

        int untyped = 0;
        for (int i = 0; i < colCount; i++)
        {
            _columnNativeTypes[i] = rows.GetFieldType(i);
            if (_columnNativeTypes[i] == StoolapType.Null)
            {
                untyped++;
            }
        }
        if (untyped == 0)
        {
            _firstRowPending = true;
            return;
        }

        _buffered = new Queue<object?[]>();
        _buffered.Enqueue(Snapshot());
        while (untyped > 0 && _buffered.Count < TypeLookAheadRows)
        {
            if (!rows.Read())
            {
                _nativeExhausted = true;
                break;
            }
            for (int i = 0; i < colCount; i++)
            {
                if (_columnNativeTypes[i] == StoolapType.Null)
                {
                    var t = rows.GetFieldType(i);
                    if (t != StoolapType.Null)
                    {
                        _columnNativeTypes[i] = t;
                        untyped--;
                    }
                }
            }
            _buffered.Enqueue(Snapshot());
        }
    }

    public override int Depth => 0;

    public override int FieldCount
    {
        get
        {
            ThrowIfClosed();
            return _columnNativeTypes.Length;
        }
    }

    public override bool HasRows
    {
        get
        {
            ThrowIfClosed();
            return _hasRows;
        }
    }

    public override bool IsClosed => _closed;

    /// <summary>Rows changed by a DML statement; -1 for a query. Still readable after <see cref="Close"/>.</summary>
    public override int RecordsAffected => _recordsAffected;

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override bool Read()
    {
        ThrowIfClosed();
        _bufferedCurrent = null;

        if (_firstRowPending)
        {
            _firstRowPending = false;
            return _currentRowValid = true;
        }
        if (_buffered is { Count: > 0 })
        {
            _bufferedCurrent = _buffered.Dequeue();
            return _currentRowValid = true;
        }
        if (_nativeExhausted)
        {
            return _currentRowValid = false;
        }
        _currentRowValid = _rows.Read();
        _nativeExhausted = !_currentRowValid;
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
        _connection.OnReaderClosed(this, _closeConnection);
    }

    /// <summary>Closes the reader on behalf of its closing connection, without calling back into it.</summary>
    internal void CloseFromConnection()
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
        CheckOrdinal(ordinal);
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
        CheckOrdinal(ordinal);
        return MapType(_columnNativeTypes[ordinal]);
    }

    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    public override bool IsDBNull(int ordinal)
    {
        CheckCurrent(ordinal);
        return _bufferedCurrent is { } row ? row[ordinal] is null : _rows.IsDBNull(ordinal);
    }

    public override object GetValue(int ordinal)
    {
        CheckCurrent(ordinal);
        var value = _bufferedCurrent is { } row ? row[ordinal] : _rows.GetValue(ordinal);
        return value ?? DBNull.Value;
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
        if (IsNative(ordinal, StoolapType.Boolean))
        {
            return _rows.GetBoolean(ordinal);
        }
        var v = Boxed(ordinal);
        return v switch
        {
            bool b => b,
            long l => l != 0,
            double d => d != 0.0,
            string s when bool.TryParse(s, out var b) => b,
            string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) => d != 0.0,
            _ => throw CannotConvert(ordinal, v, typeof(bool)),
        };
    }

    public override byte GetByte(int ordinal) => checked((byte)GetInt64(ordinal));

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException("GetBytes is not supported by stoolap; use GetValue() and read the float[] vector payload.");

    public override char GetChar(int ordinal)
    {
        var s = GetString(ordinal);
        return s.Length == 0 ? '\0' : s[0];
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException("GetChars is not supported. Use GetString() instead.");

    public override DateTime GetDateTime(int ordinal)
    {
        if (IsNative(ordinal, StoolapType.Timestamp))
        {
            return _rows.GetDateTime(ordinal);
        }
        var v = Boxed(ordinal);
        return v switch
        {
            DateTime dt => dt,
            long nanos => DateTime.UnixEpoch.AddTicks(nanos / 100L),
            string s when DateTime.TryParse(
                s, CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var dt) => dt,
            _ => throw CannotConvert(ordinal, v, typeof(DateTime)),
        };
    }

    public override decimal GetDecimal(int ordinal) => (decimal)GetDouble(ordinal);

    public override double GetDouble(int ordinal)
    {
        if (IsNative(ordinal, StoolapType.Float))
        {
            return _rows.GetDouble(ordinal);
        }
        var v = Boxed(ordinal);
        return v switch
        {
            double d => d,
            long l => l,
            bool b => b ? 1.0 : 0.0,
            string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) => d,
            _ => throw CannotConvert(ordinal, v, typeof(double)),
        };
    }

    public override float GetFloat(int ordinal) => (float)GetDouble(ordinal);

    public override Guid GetGuid(int ordinal) => Guid.Parse(GetString(ordinal));

    public override short GetInt16(int ordinal) => checked((short)GetInt64(ordinal));

    public override int GetInt32(int ordinal) => checked((int)GetInt64(ordinal));

    public override long GetInt64(int ordinal)
    {
        if (IsNative(ordinal, StoolapType.Integer))
        {
            return _rows.GetInt64(ordinal);
        }
        var v = Boxed(ordinal);
        return v switch
        {
            long l => l,
            double d => (long)d,
            bool b => b ? 1 : 0,
            string s when long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l) => l,
            string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) => (long)d,
            _ => throw CannotConvert(ordinal, v, typeof(long)),
        };
    }

    public override string GetString(int ordinal)
    {
        if (IsNative(ordinal, StoolapType.Text, StoolapType.Json))
        {
            return _rows.GetString(ordinal) ?? string.Empty;
        }
        var v = Boxed(ordinal);
        return v switch
        {
            string s => s,
            bool b => b ? "true" : "false",
            long or double => Convert.ToString(v, CultureInfo.InvariantCulture)!,
            _ => throw CannotConvert(ordinal, v, typeof(string)),
        };
    }

    /// <summary>
    /// Reads the column as <typeparamref name="T"/>, converting through the
    /// matching typed getter (so <c>GetFieldValue&lt;int&gt;</c> works on an
    /// INTEGER column). A NULL reads as <c>default</c> for a nullable or
    /// reference <typeparamref name="T"/> and throws otherwise.
    /// </summary>
    public override T GetFieldValue<T>(int ordinal)
    {
        if (IsDBNull(ordinal))
        {
            if (typeof(T) == typeof(object))
            {
                return (T)(object)DBNull.Value; // as GetValue
            }
            if (default(T) is null)
            {
                return default!;
            }
            throw NullValue(ordinal);
        }
        var target = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        object value = Type.GetTypeCode(target) switch
        {
            TypeCode.Int64 => GetInt64(ordinal),
            TypeCode.Int32 => GetInt32(ordinal),
            TypeCode.Int16 => GetInt16(ordinal),
            TypeCode.Byte => GetByte(ordinal),
            TypeCode.SByte => checked((sbyte)GetInt64(ordinal)),
            TypeCode.UInt16 => checked((ushort)GetInt64(ordinal)),
            TypeCode.UInt32 => checked((uint)GetInt64(ordinal)),
            TypeCode.UInt64 => checked((ulong)GetInt64(ordinal)),
            TypeCode.Double => GetDouble(ordinal),
            TypeCode.Single => GetFloat(ordinal),
            TypeCode.Decimal => GetDecimal(ordinal),
            TypeCode.Boolean => GetBoolean(ordinal),
            TypeCode.String => GetString(ordinal),
            TypeCode.DateTime => GetDateTime(ordinal),
            TypeCode.Char => GetChar(ordinal),
            _ when target == typeof(Guid) => GetGuid(ordinal),
            _ when target == typeof(DateTimeOffset) => new DateTimeOffset(GetDateTime(ordinal), TimeSpan.Zero),
            _ => GetValue(ordinal),
        };
        return (T)value;
    }

    /// <summary>
    /// Describes the result columns in the standard schema table layout, so
    /// <c>DataTable.Load</c> and schema-driven tools work. Column types follow
    /// <see cref="GetFieldType(int)"/>; Stoolap reports no sizes, keys or
    /// nullability for a result, so every column is reported as nullable.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2111",
        Justification = "typeof(System.Type) only declares the DataType column's type; System.Type itself is always preserved and the column just stores Type references.")]
    public override DataTable GetSchemaTable()
    {
        ThrowIfClosed();
        var table = new DataTable("SchemaTable");
        var name = table.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
        var ordinalCol = table.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
        var size = table.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
        var precision = table.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short));
        var scale = table.Columns.Add(SchemaTableColumn.NumericScale, typeof(short));
        var dataType = table.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
        var providerType = table.Columns.Add(SchemaTableColumn.ProviderType, typeof(int));
        var isLong = table.Columns.Add(SchemaTableColumn.IsLong, typeof(bool));
        var allowNull = table.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));
        var isUnique = table.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool));
        var isKey = table.Columns.Add(SchemaTableColumn.IsKey, typeof(bool));
        var baseName = table.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string));
        var isReadOnly = table.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));
        var dataTypeName = table.Columns.Add("DataTypeName", typeof(string));

        for (int i = 0; i < _columnNativeTypes.Length; i++)
        {
            var row = table.NewRow();
            row[name] = _rows.Columns[i];
            row[ordinalCol] = i;
            row[size] = -1;
            row[precision] = DBNull.Value;
            row[scale] = DBNull.Value;
            row[dataType] = MapType(_columnNativeTypes[i]);
            row[providerType] = (int)_columnNativeTypes[i];
            row[isLong] = false;
            row[allowNull] = true;
            row[isUnique] = false;
            row[isKey] = false;
            row[baseName] = _rows.Columns[i];
            row[isReadOnly] = true;
            row[dataTypeName] = _columnNativeTypes[i].ToString().ToUpperInvariant();
            table.Rows.Add(row);
        }
        return table;
    }

    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    private static Type MapType(StoolapType t) => t switch
    {
        StoolapType.Integer => typeof(long),
        StoolapType.Float => typeof(double),
        StoolapType.Text => typeof(string),
        StoolapType.Json => typeof(string),
        StoolapType.Boolean => typeof(bool),
        StoolapType.Timestamp => typeof(DateTime),
        StoolapType.Blob => typeof(float[]),
        _ => typeof(object),
    };

    private object?[] Snapshot()
    {
        var row = new object?[_columnNativeTypes.Length];
        for (int i = 0; i < row.Length; i++)
        {
            row[i] = _rows.GetValue(i);
        }
        return row;
    }

    // Typed getters follow one rule whether the current row was read ahead
    // (boxed) or is on the native cursor: a value of the getter's own type
    // is read directly (natively when possible), anything else is boxed and
    // converted by the getter's switch, and NULL throws.

    /// <summary>
    /// Checks the reader state and ordinal, refuses NULL, and returns true
    /// when the value sits on the native cursor with one of the expected
    /// types, so the getter can read it with the matching native call.
    /// </summary>
    private bool IsNative(int ordinal, StoolapType expected, StoolapType alsoExpected = StoolapType.Null)
    {
        CheckCurrent(ordinal);
        if (_bufferedCurrent is not null)
        {
            return false;
        }
        var t = _rows.GetFieldType(ordinal);
        if (t == StoolapType.Null)
        {
            throw NullValue(ordinal);
        }
        return t == expected || (alsoExpected != StoolapType.Null && t == alsoExpected);
    }

    /// <summary>The current value boxed, from the read-ahead row or the native cursor. Never NULL.</summary>
    private object Boxed(int ordinal)
    {
        var value = _bufferedCurrent is { } row ? row[ordinal] : _rows.GetValue(ordinal);
        return value ?? throw NullValue(ordinal);
    }

    private InvalidCastException NullValue(int ordinal)
        => new($"Column '{_rows.Columns[ordinal]}' (ordinal {ordinal}) is NULL. Check IsDBNull first or read it with GetValue.");

    private InvalidCastException CannotConvert(int ordinal, object value, Type target)
        => new($"Column '{_rows.Columns[ordinal]}' (ordinal {ordinal}) holds a {value.GetType().Name} that cannot be read as {target.Name}.");

    private void CheckOrdinal(int ordinal)
    {
        ThrowIfClosed();
        if ((uint)ordinal >= (uint)_columnNativeTypes.Length)
        {
            throw new IndexOutOfRangeException(
                $"Ordinal {ordinal} is out of range; the result has {_columnNativeTypes.Length} column(s).");
        }
    }

    private void CheckCurrent(int ordinal)
    {
        CheckOrdinal(ordinal);
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
