// Copyright 2026 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

using System.Reflection;
using System.Runtime.InteropServices;

namespace Stoolap.Native;

/// <summary>
/// Resolves the native libstoolap binary at runtime.
///
/// Search order:
/// <list type="number">
///   <item>STOOLAP_LIB_PATH environment variable (absolute path).</item>
///   <item>NuGet runtimes/&lt;rid&gt;/native (handled by .NET automatically).</item>
///   <item>Application base directory.</item>
///   <item>Default OS loader (LD_LIBRARY_PATH, PATH, /usr/local/lib, ...).</item>
/// </list>
/// </summary>
internal static class LibraryResolver
{
    public const string LibraryName = "stoolap";

    private static int _initialized;

    public static void EnsureRegistered()
    {
        if (Interlocked.Exchange(ref _initialized, 1) != 0)
        {
            return;
        }

        NativeLibrary.SetDllImportResolver(typeof(LibraryResolver).Assembly, Resolve);
    }

    private static nint Resolve(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (!string.Equals(libraryName, LibraryName, StringComparison.Ordinal))
        {
            return 0;
        }

        var envPath = Environment.GetEnvironmentVariable("STOOLAP_LIB_PATH");
        if (!string.IsNullOrEmpty(envPath) && File.Exists(envPath) &&
            NativeLibrary.TryLoad(envPath, out var envHandle))
        {
            return envHandle;
        }

        var fileName = GetPlatformFileName();
        var baseDir = AppContext.BaseDirectory;

        var nextToAssembly = Path.Combine(baseDir, fileName);
        if (File.Exists(nextToAssembly) && NativeLibrary.TryLoad(nextToAssembly, out var localHandle))
        {
            return localHandle;
        }

        var rid = GetRuntimeIdentifier();
        if (rid is not null)
        {
            var ridPath = Path.Combine(baseDir, "runtimes", rid, "native", fileName);
            if (File.Exists(ridPath) && NativeLibrary.TryLoad(ridPath, out var ridHandle))
            {
                return ridHandle;
            }
        }

        // Last resort: let the OS loader try (handles standard install paths
        // and any LD_LIBRARY_PATH / PATH entries the user has set).
        return NativeLibrary.TryLoad(fileName, assembly, searchPath, out var fallback)
            ? fallback
            : 0;
    }

    private static string GetPlatformFileName()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return "stoolap.dll";
        }
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return "libstoolap.dylib";
        }
        return "libstoolap.so";
    }

    private static string? GetRuntimeIdentifier()
    {
        var arch = RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.X64 => "x64",
            Architecture.Arm64 => "arm64",
            _ => null,
        };
        if (arch is null)
        {
            return null;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return $"win-{arch}";
        }
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return $"osx-{arch}";
        }
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return $"linux-{arch}";
        }
        return null;
    }
}
