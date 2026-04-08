// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Runtime.InteropServices;

namespace Stoolap.Native;

/// <summary>
/// FFI tagged union for parameter passing. Mirrors <c>StoolapValue</c> in
/// <c>src/ffi/types.rs</c>. Layout is fixed: 8-byte header (type tag + padding)
/// followed by a 16-byte payload union.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct StoolapValue
{
    public int ValueType;
    public int Padding;
    public StoolapValueData Data;
}

/// <summary>
/// Payload union for <see cref="StoolapValue"/>. The largest variant
/// (<see cref="StoolapTextData"/> / <see cref="StoolapBlobData"/>) is 16 bytes
/// on 64-bit platforms (pointer + i64 length).
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 16)]
internal struct StoolapValueData
{
    [FieldOffset(0)] public long Integer;
    [FieldOffset(0)] public double Float64;
    [FieldOffset(0)] public int Boolean;
    [FieldOffset(0)] public long TimestampNanos;
    [FieldOffset(0)] public StoolapTextData Text;
    [FieldOffset(0)] public StoolapBlobData Blob;
}

/// <summary>
/// Pointer + i64 length for text payloads. Not necessarily NUL-terminated.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct StoolapTextData
{
    public nint Ptr;
    public long Len;
}

/// <summary>
/// Pointer + i64 length for blob (vector) payloads.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct StoolapBlobData
{
    public nint Ptr;
    public long Len;
}
