// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Runtime.InteropServices;

namespace Stoolap.Native;

/// <summary>SafeHandle wrapping a <c>StoolapRows*</c> opaque pointer.</summary>
internal sealed class StoolapRowsHandle : SafeHandle
{
    public StoolapRowsHandle() : base(invalidHandleValue: 0, ownsHandle: true) { }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.stoolap_rows_close(handle);
        return true;
    }
}
