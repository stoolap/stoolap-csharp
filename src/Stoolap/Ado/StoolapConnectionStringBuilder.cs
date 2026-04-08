// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Stoolap.Ado;

/// <summary>
/// Connection string builder for stoolap. Recognizes the keys
/// <c>Data Source</c> (or <c>DataSource</c> / <c>DSN</c>).
///
/// Examples:
/// <code>
/// Data Source=memory://
/// Data Source=file:///var/lib/myapp/db
/// </code>
/// </summary>
public sealed class StoolapConnectionStringBuilder : DbConnectionStringBuilder
{
    private const string DataSourceKey = "Data Source";

    public string DataSource
    {
        get => TryGetValue(DataSourceKey, out var value) ? Convert.ToString(value) ?? string.Empty : string.Empty;
        set => this[DataSourceKey] = value;
    }

    [AllowNull]
    public override object this[string keyword]
    {
        get
        {
            // Normalize common spellings.
            if (string.Equals(keyword, "DataSource", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(keyword, "DSN", StringComparison.OrdinalIgnoreCase))
            {
                keyword = DataSourceKey;
            }
            return base[keyword];
        }
        set
        {
            if (string.Equals(keyword, "DataSource", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(keyword, "DSN", StringComparison.OrdinalIgnoreCase))
            {
                keyword = DataSourceKey;
            }
            base[keyword] = value!;
        }
    }
}
