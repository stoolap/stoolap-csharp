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
/// SafeHandle wrapping a <c>StoolapDB*</c> opaque pointer. The native library
/// is reference counted: closing one handle that shares the same engine with
/// other clones is safe.
/// </summary>
internal sealed class StoolapDbHandle : SafeHandle
{
    public StoolapDbHandle() : base(invalidHandleValue: 0, ownsHandle: true) { }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        return NativeMethods.stoolap_close(handle) == StatusCodes.Ok;
    }
}
