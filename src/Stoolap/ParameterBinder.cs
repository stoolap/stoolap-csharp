// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Runtime.InteropServices;
using System.Text;
using Stoolap.Native;

namespace Stoolap;

/// <summary>
/// Binds a managed parameter list to a pinned <see cref="StoolapValue"/>
/// buffer suitable for an FFI call. The struct is a <c>ref struct</c> so it
/// cannot escape the call site, and it owns any GC handles or unmanaged
/// allocations needed to keep string/blob payloads alive across the call.
///
/// Hot path: zero allocations. The call site provides a stack-allocated
/// scratch buffer for short text/blob parameters; only payloads that overflow
/// the scratch fall back to <see cref="Marshal.AllocHGlobal(int)"/>. For
/// numeric-only parameter lists, neither path is taken.
/// </summary>
internal ref struct ParameterBinder
{
    private const int InlineCapacity = 16;

    /// <summary>Recommended scratch buffer size for call sites. 1 KiB covers
    /// the vast majority of typical parameter lists (e.g. 7 short strings).</summary>
    public const int RecommendedScratchSize = 1024;

    // Per-binding pin handles for byte[] / float[] (only allocated lazily).
    private GCHandle[]? _pins;
    private int _pinCount;

    // Heap-allocated buffers for payloads that don't fit in scratch.
    // Lazily allocated; freed in Dispose.
    private nint[]? _allocations;
    private int _allocationCount;

    /// <summary>
    /// Walks <paramref name="parameters"/> and writes <see cref="StoolapValue"/>
    /// entries into <paramref name="dest"/>. Short text payloads are copied
    /// into <paramref name="scratch"/>; oversized ones go to HGlobal.
    /// </summary>
    public unsafe void Bind(
        ReadOnlySpan<object?> parameters,
        StoolapValue* dest,
        byte* scratch,
        int scratchSize)
    {
        int scratchUsed = 0;
        for (int i = 0; i < parameters.Length; i++)
        {
            dest[i] = ToValue(parameters[i], scratch, scratchSize, ref scratchUsed);
        }
    }

    private unsafe StoolapValue ToValue(object? p, byte* scratch, int scratchSize, ref int scratchUsed)
    {
        switch (p)
        {
            case null:
            case DBNull:
                return new StoolapValue { ValueType = (int)StoolapType.Null };

            case bool b:
                return new StoolapValue
                {
                    ValueType = (int)StoolapType.Boolean,
                    Data = new StoolapValueData { Boolean = b ? 1 : 0 },
                };

            case sbyte sb: return MakeInt(sb);
            case byte by: return MakeInt(by);
            case short sh: return MakeInt(sh);
            case ushort us: return MakeInt(us);
            case int i: return MakeInt(i);
            case uint ui: return MakeInt(ui);
            case long l: return MakeInt(l);
            case ulong ul: return MakeInt((long)ul);

            case float f: return MakeFloat(f);
            case double d: return MakeFloat(d);
            case decimal dec: return MakeFloat((double)dec);

            case string s:
                return MakeText(s, StoolapType.Text, scratch, scratchSize, ref scratchUsed);

            case DateTime dt:
                return MakeTimestamp(dt);
            case DateTimeOffset dto:
                return MakeTimestamp(dto.UtcDateTime);

            case byte[] bytes:
                return MakeBlob(bytes);

            case ReadOnlyMemory<byte> rom:
                return MakeBlobFromMemory(rom, scratch, scratchSize, ref scratchUsed);

            case Memory<byte> mem:
                return MakeBlobFromMemory(mem, scratch, scratchSize, ref scratchUsed);

            case float[] vec:
                return MakeVectorFromFloats(vec);

            case Guid g:
                return MakeText(g.ToString("D"), StoolapType.Text, scratch, scratchSize, ref scratchUsed);

            default:
                // Last resort: stringify. Avoid silent dropping.
                return MakeText(
                    Convert.ToString(p, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
                    StoolapType.Text,
                    scratch, scratchSize, ref scratchUsed);
        }
    }

    private static StoolapValue MakeInt(long v) => new()
    {
        ValueType = (int)StoolapType.Integer,
        Data = new StoolapValueData { Integer = v },
    };

    private static StoolapValue MakeFloat(double v) => new()
    {
        ValueType = (int)StoolapType.Float,
        Data = new StoolapValueData { Float64 = v },
    };

    private static StoolapValue MakeTimestamp(DateTime dt)
    {
        var utc = dt.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(dt, DateTimeKind.Utc)
            : dt.ToUniversalTime();
        var ticks = (utc - DateTime.UnixEpoch).Ticks; // 100-nanosecond units
        var nanos = ticks * 100L;
        return new StoolapValue
        {
            ValueType = (int)StoolapType.Timestamp,
            Data = new StoolapValueData { TimestampNanos = nanos },
        };
    }

    private unsafe StoolapValue MakeText(
        string s, StoolapType type, byte* scratch, int scratchSize, ref int scratchUsed)
    {
        // Worst-case UTF-8 expansion is 3 bytes per UTF-16 code unit (BMP)
        // or 4 for surrogates. GetMaxByteCount handles both.
        int maxBytes = Encoding.UTF8.GetMaxByteCount(s.Length);

        // Fast path: encode straight into the caller's stack scratch buffer.
        if (scratch != null && scratchUsed + maxBytes <= scratchSize)
        {
            int written = Encoding.UTF8.GetBytes(
                s.AsSpan(),
                new Span<byte>(scratch + scratchUsed, scratchSize - scratchUsed));
            var value = new StoolapValue
            {
                ValueType = (int)type,
                Data = new StoolapValueData
                {
                    Text = new StoolapTextData
                    {
                        Ptr = (nint)(scratch + scratchUsed),
                        Len = written,
                    },
                },
            };
            scratchUsed += written;
            return value;
        }

        // Slow path: oversized payload, fall back to HGlobal.
        int byteLen = Encoding.UTF8.GetByteCount(s);
        var buf = Marshal.AllocHGlobal(byteLen);
        var span = new Span<byte>((void*)buf, byteLen);
        Encoding.UTF8.GetBytes(s, span);
        TrackAllocation(buf);
        return new StoolapValue
        {
            ValueType = (int)type,
            Data = new StoolapValueData
            {
                Text = new StoolapTextData { Ptr = buf, Len = byteLen },
            },
        };
    }

    private StoolapValue MakeBlob(byte[] bytes)
    {
        // Pinning is cheap and avoids the copy.
        var pin = GCHandle.Alloc(bytes, GCHandleType.Pinned);
        TrackPin(pin);
        return new StoolapValue
        {
            ValueType = (int)StoolapType.Blob,
            Data = new StoolapValueData
            {
                Blob = new StoolapBlobData { Ptr = pin.AddrOfPinnedObject(), Len = bytes.Length },
            },
        };
    }

    private unsafe StoolapValue MakeBlobFromMemory(
        ReadOnlyMemory<byte> rom, byte* scratch, int scratchSize, ref int scratchUsed)
    {
        int len = rom.Length;

        // Fast path: copy small blobs into stack scratch.
        if (scratch != null && scratchUsed + len <= scratchSize)
        {
            rom.Span.CopyTo(new Span<byte>(scratch + scratchUsed, scratchSize - scratchUsed));
            var value = new StoolapValue
            {
                ValueType = (int)StoolapType.Blob,
                Data = new StoolapValueData
                {
                    Blob = new StoolapBlobData
                    {
                        Ptr = (nint)(scratch + scratchUsed),
                        Len = len,
                    },
                },
            };
            scratchUsed += len;
            return value;
        }

        // Slow path: HGlobal copy.
        var pin = rom.Pin();
        try
        {
            var buf = Marshal.AllocHGlobal(len);
            new ReadOnlySpan<byte>(pin.Pointer, len).CopyTo(new Span<byte>((void*)buf, len));
            TrackAllocation(buf);
            return new StoolapValue
            {
                ValueType = (int)StoolapType.Blob,
                Data = new StoolapValueData
                {
                    Blob = new StoolapBlobData { Ptr = buf, Len = len },
                },
            };
        }
        finally
        {
            pin.Dispose();
        }
    }

    private StoolapValue MakeVectorFromFloats(float[] vec)
    {
        // The FFI blob path treats Vector payloads as packed f32. Pinning the
        // managed float[] is sufficient: same memory representation.
        var pin = GCHandle.Alloc(vec, GCHandleType.Pinned);
        TrackPin(pin);
        return new StoolapValue
        {
            ValueType = (int)StoolapType.Blob,
            Data = new StoolapValueData
            {
                Blob = new StoolapBlobData
                {
                    Ptr = pin.AddrOfPinnedObject(),
                    Len = (long)vec.Length * sizeof(float),
                },
            },
        };
    }

    private void TrackPin(GCHandle pin)
    {
        if (_pins is null)
        {
            _pins = new GCHandle[InlineCapacity];
        }
        else if (_pinCount == _pins.Length)
        {
            Array.Resize(ref _pins, _pins.Length * 2);
        }
        _pins[_pinCount++] = pin;
    }

    private void TrackAllocation(nint ptr)
    {
        if (_allocations is null)
        {
            _allocations = new nint[InlineCapacity];
        }
        else if (_allocationCount == _allocations.Length)
        {
            Array.Resize(ref _allocations, _allocations.Length * 2);
        }
        _allocations[_allocationCount++] = ptr;
    }

    public void Dispose()
    {
        if (_pins is not null)
        {
            for (int i = 0; i < _pinCount; i++)
            {
                _pins[i].Free();
            }
        }
        if (_allocations is not null)
        {
            for (int i = 0; i < _allocationCount; i++)
            {
                Marshal.FreeHGlobal(_allocations[i]);
            }
        }
    }
}
