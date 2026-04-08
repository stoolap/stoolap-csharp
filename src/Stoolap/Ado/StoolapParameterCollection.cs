// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Collections;
using System.Data.Common;

namespace Stoolap.Ado;

/// <summary>Parameter collection for <see cref="StoolapCommand"/>.</summary>
public sealed class StoolapParameterCollection : DbParameterCollection
{
    private readonly List<StoolapParameter> _parameters = new();

    public override int Count => _parameters.Count;

    public override object SyncRoot { get; } = new();

    public override int Add(object value)
    {
        var p = Coerce(value);
        _parameters.Add(p);
        return _parameters.Count - 1;
    }

    public StoolapParameter Add(StoolapParameter parameter)
    {
        _parameters.Add(parameter);
        return parameter;
    }

    public StoolapParameter AddWithValue(string parameterName, object? value)
    {
        var p = new StoolapParameter(parameterName, value);
        _parameters.Add(p);
        return p;
    }

    public override void AddRange(Array values)
    {
        foreach (var v in values)
        {
            Add(v ?? throw new ArgumentNullException(nameof(values)));
        }
    }

    public override void Clear() => _parameters.Clear();

    public override bool Contains(object value)
        => value is StoolapParameter p && _parameters.Contains(p);

    public override bool Contains(string value) => IndexOf(value) >= 0;

    public override void CopyTo(Array array, int index)
        => ((ICollection)_parameters).CopyTo(array, index);

    public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    protected override DbParameter GetParameter(int index) => _parameters[index];

    protected override DbParameter GetParameter(string parameterName)
    {
        int idx = IndexOf(parameterName);
        if (idx < 0)
        {
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found.");
        }
        return _parameters[idx];
    }

    public override int IndexOf(object value)
        => value is StoolapParameter p ? _parameters.IndexOf(p) : -1;

    public override int IndexOf(string parameterName)
    {
        var key = StoolapParameter.NormalizeName(parameterName);
        for (int i = 0; i < _parameters.Count; i++)
        {
            if (string.Equals(_parameters[i].ParameterName, key, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
        return -1;
    }

    public override void Insert(int index, object value)
        => _parameters.Insert(index, Coerce(value));

    public override void Remove(object value)
    {
        if (value is StoolapParameter p)
        {
            _parameters.Remove(p);
        }
    }

    public override void RemoveAt(int index) => _parameters.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        int idx = IndexOf(parameterName);
        if (idx >= 0)
        {
            _parameters.RemoveAt(idx);
        }
    }

    protected override void SetParameter(int index, DbParameter value)
        => _parameters[index] = Coerce(value);

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        int idx = IndexOf(parameterName);
        if (idx < 0)
        {
            _parameters.Add(Coerce(value));
        }
        else
        {
            _parameters[idx] = Coerce(value);
        }
    }

    private static StoolapParameter Coerce(object value)
    {
        if (value is StoolapParameter sp)
        {
            return sp;
        }
        throw new InvalidCastException($"Expected StoolapParameter, got {value?.GetType().FullName ?? "null"}.");
    }
}
