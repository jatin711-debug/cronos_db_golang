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

.PARAMETER GRPCAddr
    Public client gRPC address (default: localhost:9000).

.PARAMETER HTTPAddr
    Metrics/health HTTP address (default: localhost:8080).

.PARAMETER ClusterGRPCAddr
    Internal cluster/replication gRPC address (default: localhost:10000).

.PARAMETER ClusterRaftAddr
    Cluster Raft address (default: localhost:12000).

.PARAMETER ClusterGossipAddr
    Cluster gossip address (default: localhost:14000).

.PARAMETER ClusterSeeds
    Comma-separated seed node addresses for joining an existing cluster.

.PARAMETER Cluster
    Enable clustering mode (default: true).

.EXAMPLE
    .\scripts\run-node-windows.ps1 -NodeID node1 -GRPCAddr localhost:9000

.EXAMPLE
    # Terminal 1: bootstrap leader
    .\scripts\run-node-windows.ps1 -NodeID node1 -GRPCAddr localhost:9000

    # Terminal 2: joiner
    .\scripts\run-node-windows.ps1 -NodeID node2 -GRPCAddr localhost:9001 -ClusterGRPCPort localhost:10001 -ClusterSeeds localhost:14000
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
    [string]$ClusterSeeds = "",
    [switch]$Cluster = $true
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$binary = Join-Path $repoRoot "bin" "cronos-api.exe"

if (-not (Test-Path $binary)) {
    Write-Host "Binary not found at $binary. Building..." -ForegroundColor Yellow
    & make -C $repoRoot build-api
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
if ($ClusterSeeds -ne "") {
    $args += "--cluster-seeds=$ClusterSeeds"
}

Write-Host "Starting CronosDB node $NodeID on gRPC $GRPCAddr / cluster $ClusterGRPCAddr ..." -ForegroundColor Cyan
& $binary @args
