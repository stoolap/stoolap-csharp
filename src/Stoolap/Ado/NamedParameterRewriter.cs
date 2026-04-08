// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Text;

namespace Stoolap.Ado;

/// <summary>
/// Rewrites <c>@name</c> / <c>:name</c> / <c>$name</c> placeholders in SQL into
/// positional <c>?</c> placeholders, preserving the order of first occurrence
/// for each name. Used by <see cref="StoolapCommand"/> to bridge ADO.NET's
/// named-parameter convention to stoolap's positional-only parameter model.
///
/// String literals (<c>'...'</c>), quoted identifiers (<c>"..."</c>), line
/// comments (<c>-- ...</c>), and block comments (<c>/* ... */</c>) are skipped
/// so placeholders inside them remain untouched.
///
/// Mirrors the equivalent rewriter in stoolap-node/lib/ffi.js.
/// </summary>
internal static class NamedParameterRewriter
{
    public readonly struct Result
    {
        public Result(string sql, IReadOnlyList<string> parameterNames)
        {
            Sql = sql;
            ParameterNames = parameterNames;
        }

        /// <summary>SQL with all named placeholders replaced by <c>?</c>.</summary>
        public string Sql { get; }

        /// <summary>
        /// Ordered list of parameter names (without sigil) in the order they
        /// were encountered. May contain duplicates: if the same name appears
        /// twice in the SQL, it appears twice here.
        /// </summary>
        public IReadOnlyList<string> ParameterNames { get; }
    }

    public static Result Rewrite(string sql)
    {
        // Fast path: no sigil characters, nothing to do.
        if (sql.IndexOfAny(SigilChars) < 0)
        {
            return new Result(sql, Array.Empty<string>());
        }

        var sb = new StringBuilder(sql.Length);
        var names = new List<string>(4);
        int i = 0;
        int len = sql.Length;

        while (i < len)
        {
            char ch = sql[i];

            if (ch == '\'')
            {
                int start = i;
                i++;
                while (i < len)
                {
                    if (sql[i] == '\'' && i + 1 < len && sql[i + 1] == '\'')
                    {
                        i += 2;
                        continue;
                    }
                    if (sql[i] == '\'')
                    {
                        i++;
                        break;
                    }
                    i++;
                }
                sb.Append(sql, start, i - start);
                continue;
            }

            if (ch == '"')
            {
                int start = i;
                i++;
                while (i < len)
                {
                    if (sql[i] == '"' && i + 1 < len && sql[i + 1] == '"')
                    {
                        i += 2;
                        continue;
                    }
                    if (sql[i] == '"')
                    {
                        i++;
                        break;
                    }
                    i++;
                }
                sb.Append(sql, start, i - start);
                continue;
            }

            if (ch == '-' && i + 1 < len && sql[i + 1] == '-')
            {
                int start = i;
                while (i < len && sql[i] != '\n')
                {
                    i++;
                }
                sb.Append(sql, start, i - start);
                continue;
            }

            if (ch == '/' && i + 1 < len && sql[i + 1] == '*')
            {
                int start = i;
                i += 2;
                while (i + 1 < len && !(sql[i] == '*' && sql[i + 1] == '/'))
                {
                    i++;
                }
                if (i + 1 < len)
                {
                    i += 2;
                }
                sb.Append(sql, start, i - start);
                continue;
            }

            if ((ch == '@' || ch == ':' || ch == '$') && i + 1 < len && IsIdentStart(sql[i + 1]))
            {
                int nameStart = i + 1;
                int j = nameStart;
                while (j < len && IsIdentPart(sql[j]))
                {
                    j++;
                }
                string name = sql.Substring(nameStart, j - nameStart);
                names.Add(name);
                sb.Append('?');
                i = j;
                continue;
            }

            sb.Append(ch);
            i++;
        }

        return new Result(sb.ToString(), names);
    }

    private static readonly char[] SigilChars = ['@', ':', '$'];

    private static bool IsIdentStart(char c)
        => (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';

    private static bool IsIdentPart(char c)
        => IsIdentStart(c) || (c >= '0' && c <= '9');
}
