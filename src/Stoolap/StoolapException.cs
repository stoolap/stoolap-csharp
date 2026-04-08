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
/// Thrown when a libstoolap call returns an error status. The message is
/// pulled from the per-handle error slot via <c>stoolap_*_errmsg</c>.
/// </summary>
public sealed class StoolapException : Exception
{
    public int StatusCode { get; }

    public StoolapException(string message, int statusCode = StatusCodes.Error)
        : base(message)
    {
        StatusCode = statusCode;
    }

    internal static StoolapException FromDb(nint db)
        => new(ReadCString(NativeMethods.stoolap_errmsg(db)) ?? "unknown stoolap error");

    internal static StoolapException FromStmt(nint stmt)
        => new(ReadCString(NativeMethods.stoolap_stmt_errmsg(stmt)) ?? "unknown stoolap error");

    internal static StoolapException FromTx(nint tx)
        => new(ReadCString(NativeMethods.stoolap_tx_errmsg(tx)) ?? "unknown stoolap error");

    internal static StoolapException FromRows(nint rows)
        => new(ReadCString(NativeMethods.stoolap_rows_errmsg(rows)) ?? "unknown stoolap error");

    internal static string? ReadCString(nint ptr)
    {
        if (ptr == 0)
        {
            return null;
        }
        return Marshal.PtrToStringUTF8(ptr);
    }
}
