// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Stoolap;

/// <summary>
/// Decoder for the packed binary buffer produced by <c>stoolap_rows_fetch_all</c>.
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
/// buffer is freed after parsing).
/// </summary>
internal static class BinaryRowParser
{
    /// <summary>Result of decoding a fetch-all buffer.</summary>
    internal sealed class Decoded
    {
        public string[] Columns { get; init; } = Array.Empty<string>();
        public List<object?[]> Rows { get; init; } = new();
    }

    public static Decoded Parse(ReadOnlySpan<byte> buf)
    {
        int offset = 0;
        uint colCount = ReadUInt32(buf, ref offset);
        var columns = new string[colCount];
        for (int i = 0; i < colCount; i++)
        {
            ushort nameLen = ReadUInt16(buf, ref offset);
            columns[i] = Encoding.UTF8.GetString(buf.Slice(offset, nameLen));
            offset += nameLen;
        }

        uint rowCount = ReadUInt32(buf, ref offset);
        var rows = new List<object?[]>(checked((int)rowCount));

        for (int r = 0; r < rowCount; r++)
        {
            var row = new object?[colCount];
            for (int c = 0; c < colCount; c++)
            {
                byte tag = buf[offset++];
                switch (tag)
                {
                    case 0: // NULL
                        row[c] = null;
                        break;
                    case 1: // INTEGER
                        row[c] = ReadInt64(buf, ref offset);
                        break;
                    case 2: // FLOAT
                        row[c] = ReadDouble(buf, ref offset);
                        break;
                    case 3: // TEXT
                        {
                            uint len = ReadUInt32(buf, ref offset);
                            row[c] = Encoding.UTF8.GetString(buf.Slice(offset, (int)len));
                            offset += (int)len;
                            break;
                        }
                    case 4: // BOOLEAN
                        row[c] = buf[offset++] != 0;
                        break;
                    case 5: // TIMESTAMP (nanos since unix epoch, UTC)
                        {
                            long nanos = ReadInt64(buf, ref offset);
                            row[c] = NanosToDateTime(nanos);
                            break;
                        }
                    case 6: // JSON
                        {
                            uint len = ReadUInt32(buf, ref offset);
                            row[c] = Encoding.UTF8.GetString(buf.Slice(offset, (int)len));
                            offset += (int)len;
                            break;
                        }
                    case 7: // BLOB / VECTOR (packed f32)
                        {
                            uint len = ReadUInt32(buf, ref offset);
                            if (len == 0)
                            {
                                row[c] = Array.Empty<float>();
                            }
                            else
                            {
                                int floatCount = (int)(len / sizeof(float));
                                var floats = new float[floatCount];
                                MemoryMarshal.Cast<byte, float>(buf.Slice(offset, (int)len))
                                    .CopyTo(floats);
                                row[c] = floats;
                            }
                            offset += (int)len;
                            break;
                        }
                    default:
                        throw new InvalidDataException($"Unknown stoolap value tag: {tag} at offset {offset - 1}");
                }
            }
            rows.Add(row);
        }

        return new Decoded { Columns = columns, Rows = rows };
    }

    private static DateTime NanosToDateTime(long nanos)
    {
        long ticks = nanos / 100L;
        return DateTime.UnixEpoch.AddTicks(ticks);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ReadUInt32(ReadOnlySpan<byte> buf, ref int offset)
    {
        var v = Unsafe.ReadUnaligned<uint>(ref MemoryMarshal.GetReference(buf.Slice(offset)));
        offset += 4;
        return BitConverter.IsLittleEndian ? v : System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ushort ReadUInt16(ReadOnlySpan<byte> buf, ref int offset)
    {
        var v = Unsafe.ReadUnaligned<ushort>(ref MemoryMarshal.GetReference(buf.Slice(offset)));
        offset += 2;
        return BitConverter.IsLittleEndian ? v : System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadInt64(ReadOnlySpan<byte> buf, ref int offset)
    {
        var v = Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(buf.Slice(offset)));
        offset += 8;
        return BitConverter.IsLittleEndian ? v : System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double ReadDouble(ReadOnlySpan<byte> buf, ref int offset)
    {
        var bits = ReadInt64(buf, ref offset);
        return BitConverter.Int64BitsToDouble(bits);
    }
}
