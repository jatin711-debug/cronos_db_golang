#Requires -Version 5.1
<#
.SYNOPSIS
    Run CronosDB unit tests on Windows.

.DESCRIPTION
    Go test binaries are built in a temporary directory, so they cannot find the
    bundled Rust bloom-filter DLL (internal/dedup/cronos_dedup.dll) relative to
    the repository root. This script prepends the DLL directory to PATH and then
    runs the unit-test packages.

.EXAMPLE
    .\scripts\run-tests-windows.ps1
#>

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$dllDir = Join-Path (Join-Path $repoRoot "internal") "dedup"

if (-not (Test-Path (Join-Path $dllDir "cronos_dedup.dll"))) {
    Write-Error "cronos_dedup.dll not found at $dllDir. Build it first (see internal/dedup/rust/README.md)."
}

# Preserve existing PATH and prepend the DLL directory.
$env:PATH = "$dllDir;$env:PATH"

Write-Host "Running CronosDB Windows unit tests..." -ForegroundColor Cyan

# Core packages that exercise the WAL, scheduler, dedup, partition and API layers.
$packages = @(
    "./internal/storage/...",
    "./internal/scheduler/...",
    "./internal/dedup/...",
    "./internal/partition/...",
    "./internal/cluster/...",
    "./internal/api/..."
)

& go test -count=1 @packages
if ($LASTEXITCODE -ne 0) {
    Write-Error "Tests failed with exit code $LASTEXITCODE"
}

Write-Host "All tests passed." -ForegroundColor Green
