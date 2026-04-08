// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Stoolap.Ado;

/// <summary>
/// ADO.NET parameter for stoolap. Supports both named (via <see cref="ParameterName"/>)
/// and positional binding; <see cref="StoolapCommand"/> rewrites named placeholders
/// in the SQL into positional <c>?</c> tokens before sending them to the engine.
/// </summary>
public sealed class StoolapParameter : DbParameter
{
    private string _parameterName = string.Empty;
    private object? _value;

    public StoolapParameter() { }

    public StoolapParameter(string name, object? value)
    {
        _parameterName = NormalizeName(name);
        _value = value;
    }

    public override DbType DbType { get; set; } = DbType.Object;

    public override ParameterDirection Direction
    {
        get => ParameterDirection.Input;
        set
        {
            if (value != ParameterDirection.Input)
            {
                throw new NotSupportedException("Stoolap supports only input parameters.");
            }
        }
    }

    public override bool IsNullable { get; set; } = true;

    [AllowNull]
    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = NormalizeName(value);
    }

    public override int Size { get; set; }

    [AllowNull]
    public override string SourceColumn { get; set; } = string.Empty;

    public override bool SourceColumnNullMapping { get; set; }

    public override object? Value
    {
        get => _value;
        set => _value = value;
    }

    public override void ResetDbType() => DbType = DbType.Object;

    /// <summary>
    /// Strips a leading sigil (<c>@</c>, <c>:</c>, or <c>$</c>) so the name
    /// matches the rewritten SQL produced by <see cref="NamedParameterRewriter"/>.
    /// </summary>
    internal static string NormalizeName(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return string.Empty;
        }
        char first = name[0];
        if (first == '@' || first == ':' || first == '$')
        {
            return name.Substring(1);
        }
        return name;
    }
}
