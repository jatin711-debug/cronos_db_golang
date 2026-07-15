#Requires -Version 5.1
<#
.SYNOPSIS
    Run a CronosDB node on Windows in developer mode.

.DESCRIPTION
    The Makefile targets (make node1 / make node2 / make node3) work in POSIX
    shells; this script provides a PowerShell equivalent for Windows local
    development. It ensures the binary exists and starts a single node with
    sensible defaults.

.PARAMETER NodeID
    Node identifier (default: node1).

.PARAMETER DataDir
    Data directory (default: ./data/node1).

.PARAMETER GRPCPort
    Public client gRPC port (default: 9000).

.PARAMETER HTTPPort
    Metrics/health HTTP port (default: 8080).

.PARAMETER ClusterGRPCPort
    Internal cluster/replication gRPC port (default: 10000).

.PARAMETER Cluster
    Enable clustering mode (default: true).

.PARAMETER Bootstrap
    Bootstrap a new cluster on this node (default: true for node1).

.EXAMPLE
    .\scripts\run-node-windows.ps1 -NodeID node1 -GRPCPort 9000

.EXAMPLE
    # Terminal 1: bootstrap
    .\scripts\run-node-windows.ps1 -NodeID node1 -Bootstrap

    # Terminal 2: joiner
    .\scripts\run-node-windows.ps1 -NodeID node2 -GRPCPort 9001 -ClusterGRPCPort 10001 -Bootstrap:$false
#>

[CmdletBinding()]
param(
    [string]$NodeID = "node1",
    [string]$DataDir = "./data/$NodeID",
    [string]$GRPCAddr = "localhost:9000",
    [string]$HTTPAddr = "localhost:8080",
    [string]$ClusterGRPCAddr = "localhost:10000",
    [string]$ClusterRaftAddr = "localhost:12000",
    [string]$ClusterGossipAddr = "localhost:14000",
    [switch]$Cluster = $true,
    [switch]$Bootstrap = $true
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$binary = Join-Path $repoRoot "bin" "cronos-api.exe"

if (-not (Test-Path $binary)) {
    Write-Host "Binary not found at $binary. Building..." -ForegroundColor Yellow
    & make -C $repoRoot build
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed"
    }
}

# Ensure the Rust DLL is on PATH so the binary can load it.
$dllDir = Join-Path (Join-Path $repoRoot "internal") "dedup"
$env:PATH = "$dllDir;$env:PATH"

$args = @(
    "--dev",
    "--node-id=$NodeID",
    "--data-dir=$DataDir",
    "--grpc-addr=$GRPCAddr",
    "--http-addr=$HTTPAddr",
    "--cluster-grpc-addr=$ClusterGRPCAddr",
    "--cluster-raft-addr=$ClusterRaftAddr",
    "--cluster-gossip-addr=$ClusterGossipAddr",
    "--auth-enabled=false"
)

if ($Cluster) {
    $args += "--cluster"
}
if ($Bootstrap) {
    $args += "--bootstrap"
}

Write-Host "Starting CronosDB node $NodeID on gRPC $GRPCAddr / cluster $ClusterGRPCAddr ..." -ForegroundColor Cyan
& $binary @args
