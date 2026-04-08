// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Stoolap vs Microsoft.Data.Sqlite C# benchmark.
//
// Both drivers use synchronous methods for fair comparison.
// Matches the Python benchmark.py test set and ordering.
//
// Run:  dotnet run -c Release

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Stoolap;

internal static class Program
{
    private const int RowCount = 10_000;
    private const int Iterations = 500;       // Point queries
    private const int IterationsMedium = 250; // Index scans, aggregations
    private const int IterationsHeavy = 50;   // Full scans, JOINs
    private const int Warmup = 10;

    private static int s_stoolapWins;
    private static int s_sqliteWins;

    private static string FmtUs(double us) => us.ToString("F3", CultureInfo.InvariantCulture).PadLeft(15);

    private static string FmtRatio(double stoolapUs, double sqliteUs)
    {
        if (stoolapUs <= 0 || sqliteUs <= 0)
        {
            return "      -";
        }
        double ratio = sqliteUs / stoolapUs;
        if (ratio >= 1)
        {
            return ratio.ToString("F2", CultureInfo.InvariantCulture).PadLeft(8) + "x  ";
        }
        return (1 / ratio).ToString("F2", CultureInfo.InvariantCulture).PadLeft(8) + "x* ";
    }

    private static void PrintRow(string name, double stoolapUs, double sqliteUs)
    {
        if (stoolapUs < sqliteUs) s_stoolapWins++;
        else if (sqliteUs < stoolapUs) s_sqliteWins++;

        Console.WriteLine($"{name,-28} | {FmtUs(stoolapUs)} | {FmtUs(sqliteUs)} | {FmtRatio(stoolapUs, sqliteUs)}");
    }

    private static void PrintHeader(string section)
    {
        Console.WriteLine();
        Console.WriteLine(new string('=', 80));
        Console.WriteLine(section);
        Console.WriteLine(new string('=', 80));
        Console.WriteLine($"{"Operation",-28} | {"Stoolap (μs)",15} | {"SQLite (μs)",15} | {"Ratio",10}");
        Console.WriteLine(new string('-', 80));
    }

    private static long SeedRandom(long i) => (i * 1103515245L + 12345L) & 0x7FFFFFFFL;

    /// <summary>Run <paramref name="fn"/> <paramref name="iters"/> times and return microseconds per call.</summary>
    private static double BenchUs(Action fn, int iters)
    {
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iters; i++)
        {
            fn();
        }
        sw.Stop();
        return (sw.Elapsed.TotalMilliseconds * 1000.0) / iters;
    }

    /// <summary>Drains a SQLite reader fully (mirrors fetchall()).</summary>
    private static void DrainReader(SqliteCommand cmd)
    {
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                _ = reader.GetValue(i);
            }
        }
    }

    private static int Main()
    {
        Console.WriteLine("Stoolap vs Microsoft.Data.Sqlite — C# Benchmark");
        Console.WriteLine($"Configuration: {RowCount} rows, {Iterations} iterations per test");
        Console.WriteLine("All operations are synchronous — fair comparison");
        Console.WriteLine("Ratio > 1x = Stoolap faster  |  * = SQLite faster\n");

        // --- Stoolap setup ---
        using var sdb = Database.OpenInMemory();
        sdb.Execute(@"CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            balance FLOAT NOT NULL,
            active BOOLEAN NOT NULL,
            created_at TEXT NOT NULL
        )");
        sdb.Execute("CREATE INDEX idx_users_age ON users(age)");
        sdb.Execute("CREATE INDEX idx_users_active ON users(active)");

        // --- SQLite setup ---
        using var ldb = new SqliteConnection("Data Source=:memory:");
        ldb.Open();
        Exec(ldb, "PRAGMA journal_mode=WAL");
        Exec(ldb, @"CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            balance REAL NOT NULL,
            active INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )");
        Exec(ldb, "CREATE INDEX idx_users_age ON users(age)");
        Exec(ldb, "CREATE INDEX idx_users_active ON users(active)");

        // --- Populate users ---
        using (var stoolapInsert = sdb.Prepare(
            "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)"))
        using (var sqliteTx = ldb.BeginTransaction())
        using (var sqliteInsert = ldb.CreateCommand())
        {
            sqliteInsert.CommandText =
                "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($id, $name, $email, $age, $bal, $act, $ts)";
            var p1 = sqliteInsert.CreateParameter(); p1.ParameterName = "$id"; sqliteInsert.Parameters.Add(p1);
            var p2 = sqliteInsert.CreateParameter(); p2.ParameterName = "$name"; sqliteInsert.Parameters.Add(p2);
            var p3 = sqliteInsert.CreateParameter(); p3.ParameterName = "$email"; sqliteInsert.Parameters.Add(p3);
            var p4 = sqliteInsert.CreateParameter(); p4.ParameterName = "$age"; sqliteInsert.Parameters.Add(p4);
            var p5 = sqliteInsert.CreateParameter(); p5.ParameterName = "$bal"; sqliteInsert.Parameters.Add(p5);
            var p6 = sqliteInsert.CreateParameter(); p6.ParameterName = "$act"; sqliteInsert.Parameters.Add(p6);
            var p7 = sqliteInsert.CreateParameter(); p7.ParameterName = "$ts"; sqliteInsert.Parameters.Add(p7);
            sqliteInsert.Prepare();

            for (int i = 1; i <= RowCount; i++)
            {
                int age = (int)(SeedRandom(i) % 62L) + 18;
                double balance = (SeedRandom(i * 7L) % 100000L) + (SeedRandom(i * 13L) % 100L) / 100.0;
                int active = (SeedRandom(i * 3L) % 10L) < 7 ? 1 : 0;
                string name = $"User_{i}";
                string email = $"user{i}@example.com";

                stoolapInsert.Execute((object?)(long)i, name, email, (long)age, balance, active == 1, "2024-01-01 00:00:00");

                p1.Value = i;
                p2.Value = name;
                p3.Value = email;
                p4.Value = age;
                p5.Value = balance;
                p6.Value = active;
                p7.Value = "2024-01-01 00:00:00";
                sqliteInsert.ExecuteNonQuery();
            }
            sqliteTx.Commit();
        }

        // ============================================================
        // CORE OPERATIONS
        // ============================================================
        PrintHeader("CORE OPERATIONS");

        // --- SELECT by ID ---
        {
            using var s_st = sdb.Prepare("SELECT * FROM users WHERE id = ?");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "SELECT * FROM users WHERE id = $id";
            var lp = l_cmd.CreateParameter(); lp.ParameterName = "$id"; l_cmd.Parameters.Add(lp);
            l_cmd.Prepare();

            var ids = new long[Iterations];
            for (int i = 0; i < Iterations; i++) ids[i] = (i % RowCount) + 1;

            for (int i = 0; i < Warmup; i++) { s_st.Query((object)ids[i]); lp.Value = ids[i]; DrainReader(l_cmd); }

            double s = BenchUs(() => { foreach (var id in ids) s_st.Query((object)id); }, 1) / Iterations;
            double l = BenchUs(() => { foreach (var id in ids) { lp.Value = id; DrainReader(l_cmd); } }, 1) / Iterations;
            PrintRow("SELECT by ID", s, l);
        }

        // --- SELECT by index (exact) ---
        {
            using var s_st = sdb.Prepare("SELECT * FROM users WHERE age = ?");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "SELECT * FROM users WHERE age = $age";
            var lp = l_cmd.CreateParameter(); lp.ParameterName = "$age"; l_cmd.Parameters.Add(lp);
            l_cmd.Prepare();

            var ages = new long[Iterations];
            for (int i = 0; i < Iterations; i++) ages[i] = (i % 62) + 18;

            for (int i = 0; i < Warmup; i++) { s_st.Query((object)ages[i]); lp.Value = ages[i]; DrainReader(l_cmd); }

            double s = BenchUs(() => { foreach (var a in ages) s_st.Query((object)a); }, 1) / Iterations;
            double l = BenchUs(() => { foreach (var a in ages) { lp.Value = a; DrainReader(l_cmd); } }, 1) / Iterations;
            PrintRow("SELECT by index (exact)", s, l);
        }

        // --- SELECT by index (range) ---
        {
            using var s_st = sdb.Prepare("SELECT * FROM users WHERE age >= ? AND age <= ?");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "SELECT * FROM users WHERE age >= $lo AND age <= $hi";
            var p_lo = l_cmd.CreateParameter(); p_lo.ParameterName = "$lo"; l_cmd.Parameters.Add(p_lo);
            var p_hi = l_cmd.CreateParameter(); p_hi.ParameterName = "$hi"; l_cmd.Parameters.Add(p_hi);
            l_cmd.Prepare();
            p_lo.Value = 30L; p_hi.Value = 40L;

            for (int i = 0; i < Warmup; i++) { s_st.Query(30L, 40L); DrainReader(l_cmd); }

            double s = BenchUs(() => s_st.Query(30L, 40L), Iterations);
            double l = BenchUs(() => DrainReader(l_cmd), Iterations);
            PrintRow("SELECT by index (range)", s, l);
        }

        // --- SELECT complex ---
        {
            using var s_st = sdb.Prepare(
                "SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = true ORDER BY balance DESC LIMIT 100");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "SELECT id, name, balance FROM users WHERE age >= 25 AND age <= 45 AND active = 1 ORDER BY balance DESC LIMIT 100";
            l_cmd.Prepare();

            for (int i = 0; i < Warmup; i++) { s_st.Query(); DrainReader(l_cmd); }

            double s = BenchUs(() => s_st.Query(), Iterations);
            double l = BenchUs(() => DrainReader(l_cmd), Iterations);
            PrintRow("SELECT complex", s, l);
        }

        // --- SELECT * (full scan) ---
        {
            using var s_st = sdb.Prepare("SELECT * FROM users");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "SELECT * FROM users";
            l_cmd.Prepare();

            for (int i = 0; i < Warmup; i++) { s_st.Query(); DrainReader(l_cmd); }

            double s = BenchUs(() => s_st.Query(), IterationsHeavy);
            double l = BenchUs(() => DrainReader(l_cmd), IterationsHeavy);
            PrintRow("SELECT * (full scan)", s, l);
        }

        // --- UPDATE by ID ---
        {
            using var s_st = sdb.Prepare("UPDATE users SET balance = ? WHERE id = ?");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "UPDATE users SET balance = $bal WHERE id = $id";
            var pb = l_cmd.CreateParameter(); pb.ParameterName = "$bal"; l_cmd.Parameters.Add(pb);
            var pi = l_cmd.CreateParameter(); pi.ParameterName = "$id"; l_cmd.Parameters.Add(pi);
            l_cmd.Prepare();

            var bals = new double[Iterations];
            var idsU = new long[Iterations];
            for (int i = 0; i < Iterations; i++)
            {
                bals[i] = (SeedRandom(i * 17L) % 100000L) + 0.5;
                idsU[i] = (i % RowCount) + 1;
            }

            for (int i = 0; i < Warmup; i++) { s_st.Execute(bals[i], idsU[i]); pb.Value = bals[i]; pi.Value = idsU[i]; l_cmd.ExecuteNonQuery(); }

            double s = BenchUs(() => { for (int i = 0; i < Iterations; i++) s_st.Execute(bals[i], idsU[i]); }, 1) / Iterations;
            double l = BenchUs(() => { for (int i = 0; i < Iterations; i++) { pb.Value = bals[i]; pi.Value = idsU[i]; l_cmd.ExecuteNonQuery(); } }, 1) / Iterations;
            PrintRow("UPDATE by ID", s, l);
        }

        // --- UPDATE complex ---
        {
            using var s_st = sdb.Prepare("UPDATE users SET balance = ? WHERE age >= ? AND age <= ? AND active = true");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "UPDATE users SET balance = $bal WHERE age >= $lo AND age <= $hi AND active = 1";
            var pb = l_cmd.CreateParameter(); pb.ParameterName = "$bal"; l_cmd.Parameters.Add(pb);
            var pl = l_cmd.CreateParameter(); pl.ParameterName = "$lo"; l_cmd.Parameters.Add(pl);
            var ph = l_cmd.CreateParameter(); ph.ParameterName = "$hi"; l_cmd.Parameters.Add(ph);
            l_cmd.Prepare();
            pl.Value = 27L; ph.Value = 28L;

            var bals = new double[Iterations];
            for (int i = 0; i < Iterations; i++) bals[i] = (SeedRandom(i * 23L) % 100000L) + 0.5;

            for (int i = 0; i < Warmup; i++) { s_st.Execute(bals[i], 27L, 28L); pb.Value = bals[i]; l_cmd.ExecuteNonQuery(); }

            double s = BenchUs(() => { for (int i = 0; i < Iterations; i++) s_st.Execute(bals[i], 27L, 28L); }, 1) / Iterations;
            double l = BenchUs(() => { for (int i = 0; i < Iterations; i++) { pb.Value = bals[i]; l_cmd.ExecuteNonQuery(); } }, 1) / Iterations;
            PrintRow("UPDATE complex", s, l);
        }

        // --- INSERT single ---
        {
            using var s_st = sdb.Prepare("INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "INSERT INTO users (id, name, email, age, balance, active, created_at) VALUES ($id, $name, $email, $age, $bal, $act, $ts)";
            var p1 = l_cmd.CreateParameter(); p1.ParameterName = "$id"; l_cmd.Parameters.Add(p1);
            var p2 = l_cmd.CreateParameter(); p2.ParameterName = "$name"; l_cmd.Parameters.Add(p2);
            var p3 = l_cmd.CreateParameter(); p3.ParameterName = "$email"; l_cmd.Parameters.Add(p3);
            var p4 = l_cmd.CreateParameter(); p4.ParameterName = "$age"; l_cmd.Parameters.Add(p4);
            var p5 = l_cmd.CreateParameter(); p5.ParameterName = "$bal"; l_cmd.Parameters.Add(p5);
            var p6 = l_cmd.CreateParameter(); p6.ParameterName = "$act"; l_cmd.Parameters.Add(p6);
            var p7 = l_cmd.CreateParameter(); p7.ParameterName = "$ts"; l_cmd.Parameters.Add(p7);
            l_cmd.Prepare();

            int sBase = RowCount + 1000;
            int lBase = sBase + Iterations;

            // Warm up both paths so we measure the steady-state cost.
            for (int w = 0; w < 5; w++)
            {
                long wid = lBase * 2 + w;
                s_st.Execute(wid, "warm", "warm@x", 20L, 1.0, true, "ts");
                p1.Value = wid + 1000; p2.Value = "warm"; p3.Value = "warm@x";
                p4.Value = 20L; p5.Value = 1.0; p6.Value = 1; p7.Value = "ts";
                l_cmd.ExecuteNonQuery();
            }

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < Iterations; i++)
            {
                long id = sBase + i;
                s_st.Execute(id, $"New_{id}", $"new{id}@example.com", (long)((SeedRandom(i * 29L) % 62L) + 18), 100.0, true, "2024-01-01 00:00:00");
            }
            double s = (sw.Elapsed.TotalMilliseconds * 1000.0) / Iterations;

            sw.Restart();
            for (int i = 0; i < Iterations; i++)
            {
                long id = lBase + i;
                p1.Value = id;
                p2.Value = $"New_{id}";
                p3.Value = $"new{id}@example.com";
                p4.Value = (long)((SeedRandom(i * 29L) % 62L) + 18);
                p5.Value = 100.0;
                p6.Value = 1;
                p7.Value = "2024-01-01 00:00:00";
                l_cmd.ExecuteNonQuery();
            }
            double l = (sw.Elapsed.TotalMilliseconds * 1000.0) / Iterations;

            PrintRow("INSERT single", s, l);
        }

        // --- DELETE by ID ---
        {
            using var s_st = sdb.Prepare("DELETE FROM users WHERE id = ?");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "DELETE FROM users WHERE id = $id";
            var pi = l_cmd.CreateParameter(); pi.ParameterName = "$id"; l_cmd.Parameters.Add(pi);
            l_cmd.Prepare();

            int sBase = RowCount + 1000;
            int lBase = sBase + Iterations;

            // JIT warmup: deletes against non-existent ids still go through the
            // full parse/plan/execute path, so the steady-state JIT is reached.
            for (int w = 0; w < Warmup; w++) { s_st.Execute(-1L - w); pi.Value = -1L - w; l_cmd.ExecuteNonQuery(); }

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < Iterations; i++) s_st.Execute((long)(sBase + i));
            double s = (sw.Elapsed.TotalMilliseconds * 1000.0) / Iterations;

            sw.Restart();
            for (int i = 0; i < Iterations; i++) { pi.Value = (long)(lBase + i); l_cmd.ExecuteNonQuery(); }
            double l = (sw.Elapsed.TotalMilliseconds * 1000.0) / Iterations;
            PrintRow("DELETE by ID", s, l);
        }

        // --- DELETE complex ---
        {
            using var s_st = sdb.Prepare("DELETE FROM users WHERE age >= ? AND age <= ? AND active = true");
            using var l_cmd = ldb.CreateCommand();
            l_cmd.CommandText = "DELETE FROM users WHERE age >= $lo AND age <= $hi AND active = 1";
            var pl = l_cmd.CreateParameter(); pl.ParameterName = "$lo"; pl.Value = 25L; l_cmd.Parameters.Add(pl);
            var ph = l_cmd.CreateParameter(); ph.ParameterName = "$hi"; ph.Value = 26L; l_cmd.Parameters.Add(ph);
            l_cmd.Prepare();

            // JIT warmup with a non-matching range so the table state is unchanged.
            for (int w = 0; w < Warmup; w++)
            {
                s_st.Execute(999L, 999L);
                pl.Value = 999L; ph.Value = 999L; l_cmd.ExecuteNonQuery();
            }
            pl.Value = 25L; ph.Value = 26L;

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < Iterations; i++) s_st.Execute(25L, 26L);
            double s = (sw.Elapsed.TotalMilliseconds * 1000.0) / Iterations;

            sw.Restart();
            for (int i = 0; i < Iterations; i++) l_cmd.ExecuteNonQuery();
            double l = (sw.Elapsed.TotalMilliseconds * 1000.0) / Iterations;
            PrintRow("DELETE complex", s, l);
        }

        // --- Aggregation (GROUP BY) ---
        RunQueryBench("Aggregation (GROUP BY)", sdb, ldb,
            "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age",
            "SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age",
            IterationsMedium);

        // ============================================================
        // ADVANCED OPERATIONS
        // ============================================================

        // Create orders table
        sdb.Execute(@"CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount FLOAT NOT NULL,
            status TEXT NOT NULL,
            order_date TEXT NOT NULL
        )");
        sdb.Execute("CREATE INDEX idx_orders_user_id ON orders(user_id)");
        sdb.Execute("CREATE INDEX idx_orders_status ON orders(status)");

        Exec(ldb, @"CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            order_date TEXT NOT NULL
        )");
        Exec(ldb, "CREATE INDEX idx_orders_user_id ON orders(user_id)");
        Exec(ldb, "CREATE INDEX idx_orders_status ON orders(status)");

        // Populate orders (3 per user on average)
        var statuses = new[] { "pending", "completed", "shipped", "cancelled" };
        using (var s_ord = sdb.Prepare("INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)"))
        using (var tx = ldb.BeginTransaction())
        using (var l_ord = ldb.CreateCommand())
        {
            l_ord.CommandText = "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($id, $uid, $amt, $st, $dt)";
            var p1 = l_ord.CreateParameter(); p1.ParameterName = "$id"; l_ord.Parameters.Add(p1);
            var p2 = l_ord.CreateParameter(); p2.ParameterName = "$uid"; l_ord.Parameters.Add(p2);
            var p3 = l_ord.CreateParameter(); p3.ParameterName = "$amt"; l_ord.Parameters.Add(p3);
            var p4 = l_ord.CreateParameter(); p4.ParameterName = "$st"; l_ord.Parameters.Add(p4);
            var p5 = l_ord.CreateParameter(); p5.ParameterName = "$dt"; l_ord.Parameters.Add(p5);
            l_ord.Prepare();

            int total = RowCount * 3;
            for (int i = 1; i <= total; i++)
            {
                long uid = (SeedRandom(i * 11L) % RowCount) + 1;
                double amt = (SeedRandom(i * 19L) % 990L) + 10 + (SeedRandom(i * 23L) % 100L) / 100.0;
                string st = statuses[(int)(SeedRandom(i * 31L) % 4L)];
                s_ord.Execute((long)i, uid, amt, st, "2024-01-15");

                p1.Value = (long)i; p2.Value = uid; p3.Value = amt; p4.Value = st; p5.Value = "2024-01-15";
                l_ord.ExecuteNonQuery();
            }
            tx.Commit();
        }

        PrintHeader("ADVANCED OPERATIONS");

        RunQueryBench("INNER JOIN", sdb, ldb,
            "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100",
            "SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed' LIMIT 100",
            100);

        RunQueryBench("LEFT JOIN + GROUP BY", sdb, ldb,
            "SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100",
            "SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name LIMIT 100",
            100);

        RunQueryBench("Scalar subquery", sdb, ldb,
            "SELECT name, balance, (SELECT AVG(balance) FROM users) as avg_balance FROM users WHERE balance > (SELECT AVG(balance) FROM users) LIMIT 100",
            "SELECT name, balance, (SELECT AVG(balance) FROM users) as avg_balance FROM users WHERE balance > (SELECT AVG(balance) FROM users) LIMIT 100",
            Iterations);

        RunQueryBench("IN subquery", sdb, ldb,
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100",
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed') LIMIT 100",
            10, warmup: 0);

        RunQueryBench("EXISTS subquery", sdb, ldb,
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100",
            "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 500) LIMIT 100",
            100);

        RunQueryBench("CTE + JOIN", sdb, ldb,
            "WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100",
            "WITH high_value AS (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000) SELECT u.name, h.total FROM users u INNER JOIN high_value h ON u.id = h.user_id LIMIT 100",
            20, warmup: 0);

        RunQueryBench("Window ROW_NUMBER", sdb, ldb,
            "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rank FROM users LIMIT 100",
            "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rank FROM users LIMIT 100",
            Iterations);

        RunQueryBench("Window ROW_NUMBER (PK)", sdb, ldb,
            "SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rank FROM users LIMIT 100",
            "SELECT name, ROW_NUMBER() OVER (ORDER BY id) as rank FROM users LIMIT 100",
            Iterations);

        RunQueryBench("Window PARTITION BY", sdb, ldb,
            "SELECT name, age, balance, RANK() OVER (PARTITION BY age ORDER BY balance DESC) as age_rank FROM users LIMIT 100",
            "SELECT name, age, balance, RANK() OVER (PARTITION BY age ORDER BY balance DESC) as age_rank FROM users LIMIT 100",
            Iterations);

        RunQueryBench("UNION ALL", sdb, ldb,
            "SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100",
            "SELECT name, 'high' as category FROM users WHERE balance > 50000 UNION ALL SELECT name, 'low' as category FROM users WHERE balance <= 50000 LIMIT 100",
            Iterations);

        RunQueryBench("CASE expression", sdb, ldb,
            "SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100",
            "SELECT name, CASE WHEN balance > 75000 THEN 'platinum' WHEN balance > 50000 THEN 'gold' WHEN balance > 25000 THEN 'silver' ELSE 'bronze' END as tier FROM users LIMIT 100",
            Iterations);

        RunQueryBench("Complex JOIN+GRP+HAVING", sdb, ldb,
            "SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = true AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50",
            "SELECT u.name, COUNT(DISTINCT o.id) as orders, SUM(o.amount) as total FROM users u INNER JOIN orders o ON u.id = o.user_id WHERE u.active = 1 AND o.status IN ('completed', 'shipped') GROUP BY u.id, u.name HAVING COUNT(o.id) > 1 LIMIT 50",
            20, warmup: 0);

        // --- Batch INSERT (100 rows in transaction) ---
        {
            int iters = Iterations;
            int baseId = RowCount * 10;

            var sw = Stopwatch.StartNew();
            for (int it = 0; it < iters; it++)
            {
                using var tx = sdb.Begin();
                using var stmt = sdb.Prepare("INSERT INTO orders (id, user_id, amount, status, order_date) VALUES (?, ?, ?, ?, ?)");
                for (int j = 0; j < 100; j++)
                {
                    long id = baseId + (long)it * 100 + j;
                    tx.Execute(stmt, id, 1L, 100.0, "pending", "2024-02-01");
                }
                tx.Commit();
            }
            double s = (sw.Elapsed.TotalMilliseconds * 1000.0) / iters;

            sw.Restart();
            for (int it = 0; it < iters; it++)
            {
                using var tx = ldb.BeginTransaction();
                using var l_cmd = ldb.CreateCommand();
                l_cmd.CommandText = "INSERT INTO orders (id, user_id, amount, status, order_date) VALUES ($id, $uid, $amt, $st, $dt)";
                var p1 = l_cmd.CreateParameter(); p1.ParameterName = "$id"; l_cmd.Parameters.Add(p1);
                var p2 = l_cmd.CreateParameter(); p2.ParameterName = "$uid"; p2.Value = 1L; l_cmd.Parameters.Add(p2);
                var p3 = l_cmd.CreateParameter(); p3.ParameterName = "$amt"; p3.Value = 100.0; l_cmd.Parameters.Add(p3);
                var p4 = l_cmd.CreateParameter(); p4.ParameterName = "$st"; p4.Value = "pending"; l_cmd.Parameters.Add(p4);
                var p5 = l_cmd.CreateParameter(); p5.ParameterName = "$dt"; p5.Value = "2024-02-01"; l_cmd.Parameters.Add(p5);
                l_cmd.Prepare();
                for (int j = 0; j < 100; j++)
                {
                    long id = baseId + (long)iters * 100 + (long)it * 100 + j;
                    p1.Value = id;
                    l_cmd.ExecuteNonQuery();
                }
                tx.Commit();
            }
            double l = (sw.Elapsed.TotalMilliseconds * 1000.0) / iters;
            PrintRow("Batch INSERT (100 rows)", s, l);
        }

        // ============================================================
        // BOTTLENECK HUNTERS
        // ============================================================
        PrintHeader("BOTTLENECK HUNTERS");

        RunQueryBench("DISTINCT (no ORDER)", sdb, ldb,
            "SELECT DISTINCT age FROM users", "SELECT DISTINCT age FROM users", Iterations);

        RunQueryBench("DISTINCT + ORDER BY", sdb, ldb,
            "SELECT DISTINCT age FROM users ORDER BY age", "SELECT DISTINCT age FROM users ORDER BY age", Iterations);

        RunQueryBench("COUNT DISTINCT", sdb, ldb,
            "SELECT COUNT(DISTINCT age) FROM users", "SELECT COUNT(DISTINCT age) FROM users", Iterations);

        RunQueryBench("LIKE prefix (User_1%)", sdb, ldb,
            "SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100",
            "SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100", Iterations);

        RunQueryBench("LIKE contains (%50%)", sdb, ldb,
            "SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100",
            "SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100", Iterations);

        RunQueryBench("OR conditions (3 vals)", sdb, ldb,
            "SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100",
            "SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100", Iterations);

        RunQueryBench("IN list (7 values)", sdb, ldb,
            "SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100",
            "SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100", Iterations);

        RunQueryBench("NOT IN subquery", sdb, ldb,
            "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100",
            "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE status = 'cancelled') LIMIT 100",
            10, warmup: 0);

        RunQueryBench("NOT EXISTS subquery", sdb, ldb,
            "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100",
            "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'cancelled') LIMIT 100",
            100);

        RunQueryBench("OFFSET pagination (5000)", sdb, ldb,
            "SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000",
            "SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000", Iterations);

        RunQueryBench("Multi-col ORDER BY (3)", sdb, ldb,
            "SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100",
            "SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100", Iterations);

        RunQueryBench("Self JOIN (same age)", sdb, ldb,
            "SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100",
            "SELECT u1.name, u2.name, u1.age FROM users u1 INNER JOIN users u2 ON u1.age = u2.age AND u1.id < u2.id LIMIT 100",
            100);

        RunQueryBench("Multi window funcs (3)", sdb, ldb,
            "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rn, RANK() OVER (ORDER BY balance DESC) as rnk, LAG(balance) OVER (ORDER BY balance DESC) as prev_bal FROM users LIMIT 100",
            "SELECT name, balance, ROW_NUMBER() OVER (ORDER BY balance DESC) as rn, RANK() OVER (ORDER BY balance DESC) as rnk, LAG(balance) OVER (ORDER BY balance DESC) as prev_bal FROM users LIMIT 100",
            Iterations);

        RunQueryBench("Nested subquery (3 lvl)", sdb, ldb,
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)) LIMIT 100",
            "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)) LIMIT 100",
            20, warmup: 0);

        RunQueryBench("Multi aggregates (6)", sdb, ldb,
            "SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance), COUNT(DISTINCT age) FROM users",
            "SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance), COUNT(DISTINCT age) FROM users",
            Iterations);

        RunQueryBench("COALESCE + IS NOT NULL", sdb, ldb,
            "SELECT name, COALESCE(balance, 0) as bal FROM users WHERE balance IS NOT NULL LIMIT 100",
            "SELECT name, COALESCE(balance, 0) as bal FROM users WHERE balance IS NOT NULL LIMIT 100",
            Iterations);

        RunQueryBench("Expr in WHERE (funcs)", sdb, ldb,
            "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100",
            "SELECT * FROM users WHERE LENGTH(name) > 7 AND UPPER(name) LIKE 'USER_%' LIMIT 100",
            Iterations);

        RunQueryBench("Math expressions", sdb, ldb,
            "SELECT name, balance * 1.1 as new_bal, ROUND(balance / 1000, 2) as k_bal, ABS(balance - 50000) as diff FROM users LIMIT 100",
            "SELECT name, balance * 1.1 as new_bal, ROUND(balance / 1000, 2) as k_bal, ABS(balance - 50000) as diff FROM users LIMIT 100",
            Iterations);

        RunQueryBench("String concat (||)", sdb, ldb,
            "SELECT name || ' (' || email || ')' as full_info FROM users LIMIT 100",
            "SELECT name || ' (' || email || ')' as full_info FROM users LIMIT 100",
            Iterations);

        RunQueryBench("Large result (no LIMIT)", sdb, ldb,
            "SELECT id, name, balance FROM users WHERE active = true",
            "SELECT id, name, balance FROM users WHERE active = 1",
            20, warmup: 5);

        RunQueryBench("Multiple CTEs (2)", sdb, ldb,
            "WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50",
            "WITH young AS (SELECT * FROM users WHERE age < 30), rich AS (SELECT * FROM users WHERE balance > 70000) SELECT y.name, r.name FROM young y INNER JOIN rich r ON y.id = r.id LIMIT 50",
            100);

        RunQueryBench("Correlated in SELECT", sdb, ldb,
            "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100",
            "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u LIMIT 100",
            100, warmup: 5);

        RunQueryBench("BETWEEN (non-indexed)", sdb, ldb,
            "SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100",
            "SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100",
            Iterations);

        RunQueryBench("GROUP BY (2 columns)", sdb, ldb,
            "SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active",
            "SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active",
            Iterations);

        RunQueryBench("CROSS JOIN (limited)", sdb, ldb,
            "SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100",
            "SELECT u.name, o.status FROM users u CROSS JOIN (SELECT DISTINCT status FROM orders) o LIMIT 100",
            Iterations);

        RunQueryBench("Derived table (FROM sub)", sdb, ldb,
            "SELECT t.age_group, COUNT(*) FROM (SELECT CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END as age_group FROM users) t GROUP BY t.age_group",
            "SELECT t.age_group, COUNT(*) FROM (SELECT CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END as age_group FROM users) t GROUP BY t.age_group",
            Iterations);

        RunQueryBench("Window ROWS frame", sdb, ldb,
            "SELECT name, balance, SUM(balance) OVER (ORDER BY balance ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as rolling_sum FROM users LIMIT 100",
            "SELECT name, balance, SUM(balance) OVER (ORDER BY balance ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as rolling_sum FROM users LIMIT 100",
            Iterations);

        RunQueryBench("HAVING complex", sdb, ldb,
            "SELECT age FROM users GROUP BY age HAVING COUNT(*) > 100 AND AVG(balance) > 40000",
            "SELECT age FROM users GROUP BY age HAVING COUNT(*) > 100 AND AVG(balance) > 40000",
            Iterations);

        RunQueryBench("Compare with subquery", sdb, ldb,
            "SELECT * FROM users WHERE balance > (SELECT AVG(amount) * 100 FROM orders) LIMIT 100",
            "SELECT * FROM users WHERE balance > (SELECT AVG(amount) * 100 FROM orders) LIMIT 100",
            Iterations);

        // ============================================================
        // Summary
        // ============================================================
        Console.WriteLine();
        Console.WriteLine(new string('=', 80));
        Console.WriteLine($"SCORE: Stoolap {s_stoolapWins} wins  |  SQLite {s_sqliteWins} wins");
        Console.WriteLine();
        Console.WriteLine("NOTES:");
        Console.WriteLine("- Both drivers use synchronous methods — fair comparison");
        Console.WriteLine("- Stoolap: MVCC, parallel execution, columnar indexes");
        Console.WriteLine("- SQLite: WAL mode, in-memory, Microsoft.Data.Sqlite");
        Console.WriteLine("- Ratio > 1x = Stoolap faster  |  * = SQLite faster");
        Console.WriteLine(new string('=', 80));

        return 0;
    }

    private static void Exec(SqliteConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void RunQueryBench(string name, Database sdb, SqliteConnection ldb,
        string stoolapSql, string sqliteSql, int iters, int warmup = Warmup)
    {
        using var s_st = sdb.Prepare(stoolapSql);
        using var l_cmd = ldb.CreateCommand();
        l_cmd.CommandText = sqliteSql;
        l_cmd.Prepare();

        for (int i = 0; i < warmup; i++)
        {
            s_st.Query();
            DrainReader(l_cmd);
        }

        double s = BenchUs(() => s_st.Query(), iters);
        double l = BenchUs(() => DrainReader(l_cmd), iters);
        PrintRow(name, s, l);
    }
}
