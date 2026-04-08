// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

namespace Stoolap;

/// <summary>
/// Materialized result set produced by <c>Database.Query()</c>. Decoded from
/// the binary <c>stoolap_rows_fetch_all</c> buffer in a single pass with no
/// per-row P/Invoke crossings.
/// </summary>
public sealed class QueryResult
{
    public IReadOnlyList<string> Columns { get; }
    public IReadOnlyList<object?[]> Rows { get; }

    public int RowCount => Rows.Count;
    public int ColumnCount => Columns.Count;

    internal QueryResult(IReadOnlyList<string> columns, IReadOnlyList<object?[]> rows)
    {
        Columns = columns;
        Rows = rows;
    }

    /// <summary>Returns the row at <paramref name="index"/>.</summary>
    public object?[] this[int index] => Rows[index];

    /// <summary>Returns the value at the given row/column index.</summary>
    public object? this[int row, int column] => Rows[row][column];

    /// <summary>Returns the value at the given row/column name.</summary>
    public object? this[int row, string column]
    {
        get
        {
            int idx = ColumnIndex(column);
            return Rows[row][idx];
        }
    }

    public int ColumnIndex(string name)
    {
        for (int i = 0; i < Columns.Count; i++)
        {
            if (string.Equals(Columns[i], name, StringComparison.Ordinal))
            {
                return i;
            }
        }
        throw new ArgumentException($"Unknown column: {name}", nameof(name));
    }
}
