// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Data;
using System.Data.Common;

namespace Stoolap.Ado;

/// <summary>ADO.NET wrapper around a stoolap <see cref="Stoolap.Transaction"/>.</summary>
public sealed class StoolapTransaction : DbTransaction
{
    private readonly StoolapConnection _connection;
    private readonly IsolationLevel _isolation;

    internal StoolapTransaction(StoolapConnection connection, Transaction inner, IsolationLevel isolation)
    {
        _connection = connection;
        Inner = inner;
        _isolation = isolation;
    }

    /// <summary>The underlying high-level <see cref="Stoolap.Transaction"/>.</summary>
    internal Transaction Inner { get; }

    protected override DbConnection? DbConnection => _connection;

    public override IsolationLevel IsolationLevel => _isolation;

    public override void Commit() => Inner.Commit();

    public override void Rollback() => Inner.Rollback();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Inner.Dispose();
        }
        base.Dispose(disposing);
    }
}
