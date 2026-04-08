// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

namespace Stoolap.Native;

/// <summary>
/// Status codes returned by libstoolap functions. Mirrors src/ffi/mod.rs.
/// </summary>
internal static class StatusCodes
{
    public const int Ok = 0;
    public const int Error = 1;
    public const int Row = 100;
    public const int Done = 101;
}

/// <summary>
/// Value type discriminants. Mirrors STOOLAP_TYPE_* in src/ffi/mod.rs.
/// </summary>
public enum StoolapType
{
    Null = 0,
    Integer = 1,
    Float = 2,
    Text = 3,
    Boolean = 4,
    Timestamp = 5,
    Json = 6,
    Blob = 7,
}

/// <summary>
/// Transaction isolation levels. Mirrors STOOLAP_ISOLATION_* in src/ffi/mod.rs.
/// </summary>
public enum StoolapIsolationLevel
{
    ReadCommitted = 0,
    Snapshot = 1,
}
