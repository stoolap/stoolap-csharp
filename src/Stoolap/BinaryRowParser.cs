// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Stoolap;

/// <summary>
/// Decoder for the packed binary result format shared by
/// <c>stoolap_rows_fetch_all</c> and the <c>stoolap_*_query_all</c> one-call
/// queries.
///
/// Format (little-endian throughout):
/// <code>
/// u32              column_count
/// repeat column_count:
///     u16          name_len
///     u8[name_len] name_bytes (UTF-8)
/// u32              row_count
/// repeat row_count * column_count:
///     u8           type_tag
///     payload (varies, see below)
///
/// Payloads:
///     NULL(0):       (none)
///     INTEGER(1):    i64
///     FLOAT(2):      f64
///     TEXT(3):       u32 len + u8[len]
///     BOOLEAN(4):    u8
///     TIMESTAMP(5):  i64 (nanoseconds since unix epoch, UTC)
///     JSON(6):       u32 len + u8[len]
///     BLOB(7):       u32 len + u8[len]  (vector payload, packed f32)
/// </code>
///
/// Decodes with zero per-cell P/Invoke calls and zero managed copies for
/// numerics; one allocation per text/blob value (unavoidable: the source
/// buffer belongs to the native handle and is reused by its next call).
/// </summary>
internal static class BinaryRowParser
{
    // Shared boxes: a BOOLEAN cell costs no allocation.
    private static readonly object BoxedTrue = true;
    private static readonly object BoxedFalse = false;

    /// <summary>
    /// Decodes a whole packed buffer into a <see cref="QueryResult"/>. When
    /// <paramref name="cache"/> is given and the buffer's column header is
    /// byte-identical to the cached one, the cached column names are reused
    /// instead of decoding them again.
    /// </summary>
    public static QueryResult Parse(ReadOnlySpan<byte> buf, ColumnHeaderCache? cache)
    {
        int offset = SkipHeader(buf, out int colCount);
        var columns = cache is null
            ? DecodeColumns(buf, colCount)
            : cache.Get(buf.Slice(0, offset), colCount);

        int rowCount = checked((int)ReadUInt32(buf, ref offset));
        var rows = new object?[rowCount][];
        for (int r = 0; r < rowCount; r++)
        {
            var row = new object?[colCount];
            for (int c = 0; c < colCount; c++)
            {
                row[c] = ReadValue(buf, ref offset);
            }
            rows[r] = row;
        }
        return new QueryResult(columns, rows);
    }

    /// <summary>
    /// Walks the column header. Returns the offset of the <c>row_count</c>
    /// field (i.e. the header's byte length).
    /// </summary>
    public static int SkipHeader(ReadOnlySpan<byte> buf, out int colCount)
    {
        int offset = 0;
        colCount = checked((int)ReadUInt32(buf, ref offset));
        for (int i = 0; i < colCount; i++)
        {
            int nameLen = ReadUInt16(buf, ref offset);
            offset += nameLen;
        }
        return offset;
    }

    public static string[] DecodeColumns(ReadOnlySpan<byte> buf, int colCount)
    {
        var columns = new string[colCount];
        int offset = 4;
        for (int i = 0; i < colCount; i++)
        {
            ushort nameLen = ReadUInt16(buf, ref offset);
            columns[i] = Encoding.UTF8.GetString(buf.Slice(offset, nameLen));
            offset += nameLen;
        }
        return columns;
    }

    /// <summary>Decodes the tagged value at <paramref name="offset"/> into a boxed object.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? ReadValue(ReadOnlySpan<byte> buf, ref int offset)
    {
        byte tag = buf[offset++];
        switch (tag)
        {
            case 0: // NULL
                return null;
            case 1: // INTEGER
                return ReadInt64(buf, ref offset);
            case 2: // FLOAT
                return ReadDouble(buf, ref offset);
            case 3: // TEXT
            case 6: // JSON
                {
                    int len = checked((int)ReadUInt32(buf, ref offset));
                    var s = Encoding.UTF8.GetString(buf.Slice(offset, len));
                    offset += len;
                    return s;
                }
            case 4: // BOOLEAN
                return buf[offset++] != 0 ? BoxedTrue : BoxedFalse;
            case 5: // TIMESTAMP (nanos since unix epoch, UTC)
                return NanosToDateTime(ReadInt64(buf, ref offset));
            case 7: // BLOB / VECTOR (packed f32)
                {
                    int len = checked((int)ReadUInt32(buf, ref offset));
                    var floats = ReadVector(buf.Slice(offset, len));
                    offset += len;
                    return floats;
                }
            default:
                throw new InvalidDataException($"Unknown stoolap value tag: {tag} at offset {offset - 1}");
        }
    }

    public static float[] ReadVector(ReadOnlySpan<byte> payload)
    {
        if (payload.Length == 0)
        {
            return Array.Empty<float>();
        }
        var floats = new float[payload.Length / sizeof(float)];
        MemoryMarshal.Cast<byte, float>(payload).CopyTo(floats);
        return floats;
    }

    public static DateTime NanosToDateTime(long nanos) => DateTime.UnixEpoch.AddTicks(nanos / 100L);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ReadUInt32(ReadOnlySpan<byte> buf, ref int offset)
    {
        var v = BinaryPrimitives.ReadUInt32LittleEndian(buf.Slice(offset));
        offset += 4;
        return v;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ushort ReadUInt16(ReadOnlySpan<byte> buf, ref int offset)
    {
        var v = BinaryPrimitives.ReadUInt16LittleEndian(buf.Slice(offset));
        offset += 2;
        return v;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64(ReadOnlySpan<byte> buf, ref int offset)
    {
        var v = BinaryPrimitives.ReadInt64LittleEndian(buf.Slice(offset));
        offset += 8;
        return v;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double ReadDouble(ReadOnlySpan<byte> buf, ref int offset)
        => BitConverter.Int64BitsToDouble(ReadInt64(buf, ref offset));
}

/// <summary>
/// Remembers the last column header seen by one handle (a prepared statement
/// or a connection) so repeated queries with the same shape skip decoding the
/// column names. A header that differs in any byte (e.g. after a schema
/// change) is decoded and replaces the cached one.
/// </summary>
internal sealed class ColumnHeaderCache
{
    private byte[] _header = Array.Empty<byte>();
    private IReadOnlyList<string> _columns = Array.Empty<string>();

    public IReadOnlyList<string> Get(ReadOnlySpan<byte> header, int colCount)
    {
        if (header.SequenceEqual(_header))
        {
            return _columns;
        }
        var columns = Array.AsReadOnly(BinaryRowParser.DecodeColumns(header, colCount));
        _header = header.ToArray();
        _columns = columns;
        return columns;
    }
}
