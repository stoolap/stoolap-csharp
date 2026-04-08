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
/// SafeHandle wrapping a <c>StoolapTx*</c>. The native API consumes the
/// transaction on commit/rollback, so callers transition this handle out of
/// the SafeHandle's ownership before invoking those entry points; this finalizer
/// only fires when the user drops the handle without committing or rolling back,
/// which is treated as a rollback.
/// </summary>
internal sealed class StoolapTxHandle : SafeHandle
{
    public StoolapTxHandle() : base(invalidHandleValue: 0, ownsHandle: true) { }

    public override bool IsInvalid => handle == 0;

    protected override bool ReleaseHandle()
    {
        NativeMethods.stoolap_tx_rollback(handle);
        return true;
    }

    /// <summary>
    /// Releases ownership of the underlying pointer without invoking the
    /// destructor. Used right before commit/rollback, both of which consume
    /// the transaction handle in the native layer.
    /// </summary>
    public nint Detach()
    {
        var raw = handle;
        SetHandleAsInvalid();
        return raw;
    }
}
