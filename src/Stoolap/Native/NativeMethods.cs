// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Stoolap.Native;

/// <summary>
/// 1:1 P/Invoke surface for libstoolap. Mirrors <c>src/ffi/</c>.
///
/// All entry points use the source-generated <see cref="LibraryImportAttribute"/>
/// pipeline so there is no per-call IL stub: marshalling code is generated
/// at compile time and is AOT-safe.
///
/// Strings are passed as UTF-8 (<see cref="Utf8StringMarshaller"/>) because
/// stoolap is UTF-8 throughout, eliminating the UTF-16 round trip a regular
/// <c>DllImport</c> would force.
/// </summary>
internal static partial class NativeMethods
{
    private const string Lib = LibraryResolver.LibraryName;

    static NativeMethods()
    {
        LibraryResolver.EnsureRegistered();
    }

    /// <summary>Forces the static constructor (and resolver registration) to run.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static void EnsureLoaded() { }

    // ----- version / database lifecycle -----

    [LibraryImport(Lib, EntryPoint = "stoolap_version")]
    public static partial nint stoolap_version();

    [LibraryImport(Lib, EntryPoint = "stoolap_open", StringMarshalling = StringMarshalling.Utf8)]
    public static partial int stoolap_open(string dsn, out nint outDb);

    [LibraryImport(Lib, EntryPoint = "stoolap_open_in_memory")]
    public static partial int stoolap_open_in_memory(out nint outDb);

    [LibraryImport(Lib, EntryPoint = "stoolap_close")]
    public static partial int stoolap_close(nint db);

    [LibraryImport(Lib, EntryPoint = "stoolap_clone")]
    public static partial int stoolap_clone(nint db, out nint outDb);

    [LibraryImport(Lib, EntryPoint = "stoolap_errmsg")]
    public static partial nint stoolap_errmsg(nint db);

    [LibraryImport(Lib, EntryPoint = "stoolap_string_free")]
    public static partial void stoolap_string_free(nint s);

    // ----- exec / query (no params and with params) -----

    [LibraryImport(Lib, EntryPoint = "stoolap_exec", StringMarshalling = StringMarshalling.Utf8)]
    public static partial int stoolap_exec(nint db, string sql, out long rowsAffected);

    [LibraryImport(Lib, EntryPoint = "stoolap_exec_params", StringMarshalling = StringMarshalling.Utf8)]
    public static unsafe partial int stoolap_exec_params(
        nint db, string sql, StoolapValue* parameters, int paramsLen, out long rowsAffected);

    [LibraryImport(Lib, EntryPoint = "stoolap_query", StringMarshalling = StringMarshalling.Utf8)]
    public static partial int stoolap_query(nint db, string sql, out nint outRows);

    [LibraryImport(Lib, EntryPoint = "stoolap_query_params", StringMarshalling = StringMarshalling.Utf8)]
    public static unsafe partial int stoolap_query_params(
        nint db, string sql, StoolapValue* parameters, int paramsLen, out nint outRows);

    // ----- prepared statements -----

    [LibraryImport(Lib, EntryPoint = "stoolap_prepare", StringMarshalling = StringMarshalling.Utf8)]
    public static partial int stoolap_prepare(nint db, string sql, out nint outStmt);

    [LibraryImport(Lib, EntryPoint = "stoolap_stmt_exec")]
    public static unsafe partial int stoolap_stmt_exec(
        nint stmt, StoolapValue* parameters, int paramsLen, out long rowsAffected);

    [LibraryImport(Lib, EntryPoint = "stoolap_stmt_query")]
    public static unsafe partial int stoolap_stmt_query(
        nint stmt, StoolapValue* parameters, int paramsLen, out nint outRows);

    [LibraryImport(Lib, EntryPoint = "stoolap_stmt_sql")]
    public static partial nint stoolap_stmt_sql(nint stmt);

    [LibraryImport(Lib, EntryPoint = "stoolap_stmt_finalize")]
    public static partial void stoolap_stmt_finalize(nint stmt);

    [LibraryImport(Lib, EntryPoint = "stoolap_stmt_errmsg")]
    public static partial nint stoolap_stmt_errmsg(nint stmt);

    // ----- transactions -----

    [LibraryImport(Lib, EntryPoint = "stoolap_begin")]
    public static partial int stoolap_begin(nint db, out nint outTx);

    [LibraryImport(Lib, EntryPoint = "stoolap_begin_with_isolation")]
    public static partial int stoolap_begin_with_isolation(nint db, int isolation, out nint outTx);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_exec", StringMarshalling = StringMarshalling.Utf8)]
    public static partial int stoolap_tx_exec(nint tx, string sql, out long rowsAffected);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_exec_params", StringMarshalling = StringMarshalling.Utf8)]
    public static unsafe partial int stoolap_tx_exec_params(
        nint tx, string sql, StoolapValue* parameters, int paramsLen, out long rowsAffected);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_query", StringMarshalling = StringMarshalling.Utf8)]
    public static partial int stoolap_tx_query(nint tx, string sql, out nint outRows);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_query_params", StringMarshalling = StringMarshalling.Utf8)]
    public static unsafe partial int stoolap_tx_query_params(
        nint tx, string sql, StoolapValue* parameters, int paramsLen, out nint outRows);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_stmt_exec")]
    public static unsafe partial int stoolap_tx_stmt_exec(
        nint tx, nint stmt, StoolapValue* parameters, int paramsLen, out long rowsAffected);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_stmt_query")]
    public static unsafe partial int stoolap_tx_stmt_query(
        nint tx, nint stmt, StoolapValue* parameters, int paramsLen, out nint outRows);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_commit")]
    public static partial int stoolap_tx_commit(nint tx);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_rollback")]
    public static partial int stoolap_tx_rollback(nint tx);

    [LibraryImport(Lib, EntryPoint = "stoolap_tx_errmsg")]
    public static partial nint stoolap_tx_errmsg(nint tx);

    // ----- rows iteration -----

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_next")]
    public static partial int stoolap_rows_next(nint rows);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_close")]
    public static partial void stoolap_rows_close(nint rows);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_count")]
    public static partial int stoolap_rows_column_count(nint rows);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_name")]
    public static partial nint stoolap_rows_column_name(nint rows, int index);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_type")]
    public static partial int stoolap_rows_column_type(nint rows, int index);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_int64")]
    public static partial long stoolap_rows_column_int64(nint rows, int index);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_double")]
    public static partial double stoolap_rows_column_double(nint rows, int index);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_text")]
    public static partial nint stoolap_rows_column_text(nint rows, int index, out long outLen);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_bool")]
    public static partial int stoolap_rows_column_bool(nint rows, int index);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_timestamp")]
    public static partial long stoolap_rows_column_timestamp(nint rows, int index);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_blob")]
    public static partial nint stoolap_rows_column_blob(nint rows, int index, out long outLen);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_column_is_null")]
    public static partial int stoolap_rows_column_is_null(nint rows, int index);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_affected")]
    public static partial long stoolap_rows_affected(nint rows);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_fetch_all")]
    public static partial int stoolap_rows_fetch_all(nint rows, out nint outBuf, out long outLen);

    [LibraryImport(Lib, EntryPoint = "stoolap_buffer_free")]
    public static partial void stoolap_buffer_free(nint buf, long len);

    [LibraryImport(Lib, EntryPoint = "stoolap_rows_errmsg")]
    public static partial nint stoolap_rows_errmsg(nint rows);
}
