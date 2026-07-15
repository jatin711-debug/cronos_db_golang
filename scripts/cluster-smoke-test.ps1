#Requires -Version 7.2
<#
.SYNOPSIS
    CronosDB cluster smoke test for Windows direct-binary deployments.

.DESCRIPTION
    Starts a 3-node RF=3/minISR=2 cluster using the locally built binary,
    publishes events, verifies replication lag is zero, kills the leader,
    verifies failover, kills a second node and verifies writes fail closed,
    then restarts a follower and verifies catch-up.

    Run from the repo root in Git Bash/PowerShell after `make build`.
#>
param(
    [string]$Binary = "bin/cronos-api.exe",
    [string]$LoadTestBinary = "bin/cluster_loadtest.exe",
    [string]$DataRoot = "./data/smoke",
    [int]$PartitionCount = 1,
    [int]$GrpcPortBase = 29000,
    [int]$HttpPortBase = 28000,
    [int]$GossipPortBase = 27946,
    [int]$ClusterGrpcPortBase = 27966,
    [int]$RaftPortBase = 27986
)

$ErrorActionPreference = "Stop"

function Start-Node($nodeId, $index) {
    $grpc = "127.0.0.1:$($GrpcPortBase + $index)"
    $http = "127.0.0.1:$($HttpPortBase + $index)"
    $gossip = "127.0.0.1:$($GossipPortBase + $index)"
    $clusterGrpc = "127.0.0.1:$($ClusterGrpcPortBase + $index)"
    $raft = "127.0.0.1:$($RaftPortBase + $index)"
    $dataDir = "$DataRoot/$nodeId"
    $null = New-Item -ItemType Directory -Force -Path $dataDir
    $seeds = if ($index -eq 0) { "" } else { "--cluster-seeds=127.0.0.1:$GossipPortBase" }

    $args = @(
        "--dev",
        "--node-id=$nodeId",
        "--auth-enabled=false",
        "--cluster",
        "--partition-count=$PartitionCount",
        "--replication-factor=3",
        "--min-insync-replicas=2",
        "--fsync-mode=periodic",
        "--flush-interval=100",
        "--data-dir=$dataDir",
        "--grpc-addr=$grpc",
        "--http-addr=$http",
        "--cluster-gossip-addr=$gossip",
        "--cluster-grpc-addr=$clusterGrpc",
        "--cluster-raft-addr=$raft"
    )
    if ($seeds) { $args += $seeds }

    $logOut = "$DataRoot/$nodeId.out.log"
    $logErr = "$DataRoot/$nodeId.err.log"
    $proc = Start-Process -FilePath $Binary -ArgumentList $args -PassThru `
        -RedirectStandardOutput $logOut -RedirectStandardError $logErr `
        -WindowStyle Hidden
    return @{ Proc = $proc; NodeId = $nodeId; Index = $index; HTTP = $http; GRPC = $grpc; LogOut = $logOut; LogErr = $logErr }
}

function Wait-Healthy($node, $timeoutSeconds = 30) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $timeoutSeconds) {
        try {
            $r = Invoke-WebRequest -Uri "http://$($node.HTTP)/health" -UseBasicParsing -TimeoutSec 2
            if ($r.StatusCode -eq 200) { return }
        } catch {}
        Start-Sleep -Milliseconds 200
    }
    throw "Node $($node.NodeId) did not become healthy within ${timeoutSeconds}s"
}

function Get-Metrics($httpAddr) {
    try {
        return (Invoke-WebRequest -Uri "http://$httpAddr/metrics" -UseBasicParsing -TimeoutSec 2).Content
    } catch {
        return $null
    }
}

function Find-Leader($nodes) {
    # With partition-count=1, the leader exposes cronos_replication_lag for partition 0.
    foreach ($n in $nodes) {
        $m = Get-Metrics $n.HTTP
        if ($m -and $m -match 'cronos_replication_lag\{partition="0"') {
            return $n
        }
    }
    return $null
}

function Invoke-Publish($nodes, $topic, $count = 10) {
    $grpc = ($nodes | Select-Object -First 1).GRPC
    $http = ($nodes | Select-Object -First 1).HTTP
    # Use cluster_loadtest in single-topic batch mode to publish a small batch.
    $args = @(
        "-node1-grpc=$grpc",
        "-node1-http=$http",
        "-nodes=3",
        "-publishers=1",
        "-events=$count",
        "-payload=64",
        "-delay=0",
        "-topic=$topic",
        "-round-robin=true",
        "-batch",
        "-batch-size=$count",
        "-allow-duplicate=true",
        "-partition-count=$PartitionCount"
    )
    & $LoadTestBinary @args | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "Publish failed with exit code $LASTEXITCODE"
    }
}

function Test-ReplicationLagZero($nodes, $timeoutSeconds = 20) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $timeoutSeconds) {
        $leader = Find-Leader $nodes
        if ($leader) {
            $m = Get-Metrics $leader.HTTP
            if ($m -and $m -match 'cronos_replication_lag\{[^}]+partition="0"[^}]*\}\s+(\d+(?:\.\d+)?)') {
                $lag = [double]$Matches[1]
                if ($lag -eq 0) { return $true }
            }
        }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

# --- Cleanup and setup ---
$nodes = @()
try {
    if (-not (Test-Path $Binary)) { throw "Binary not found: $Binary. Run 'make build' first." }
    if (-not (Test-Path $LoadTestBinary)) { throw "Load-test binary not found: $LoadTestBinary. Run 'make build' first." }

    if (Test-Path $DataRoot) {
        Remove-Item -Recurse -Force $DataRoot
    }

    Write-Host "Starting 3-node cluster..."
    for ($i = 0; $i -lt 3; $i++) {
        $nodes += Start-Node "node$($i+1)" $i
        Start-Sleep -Milliseconds 500
    }

    Write-Host "Waiting for nodes to become healthy..."
    foreach ($n in $nodes) { Wait-Healthy $n }

    # Allow cluster to settle and elect raft leader.
    Start-Sleep -Seconds 3

    Write-Host "Publishing baseline batch..."
    Invoke-Publish $nodes "smoke-baseline" 10

    Write-Host "Waiting for replication lag to reach zero..."
    if (-not (Test-ReplicationLagZero $nodes)) {
        throw "Replication lag did not reach zero after baseline publish"
    }
    Write-Host "OK: replication lag is 0"

    # --- Kill leader ---
    $leader = Find-Leader $nodes
    if (-not $leader) { throw "Could not identify leader" }
    Write-Host "Killing leader $($leader.NodeId)..."
    Stop-Process -Id $leader.Proc.Id -Force
    $leader.Proc.WaitForExit()

    $remaining = $nodes | Where-Object { $_.NodeId -ne $leader.NodeId }

    Write-Host "Waiting for failover..."
    Start-Sleep -Seconds 5

    Write-Host "Publishing post-failover batch..."
    Invoke-Publish $remaining "smoke-failover" 10

    if (-not (Test-ReplicationLagZero $remaining)) {
        throw "Replication lag did not reach zero after failover"
    }
    Write-Host "OK: failover succeeded and lag is 0"

    # --- Kill second node to drop below minISR ---
    $second = $remaining[0]
    Write-Host "Killing second node $($second.NodeId) to drop below minISR..."
    Stop-Process -Id $second.Proc.Id -Force
    $second.Proc.WaitForExit()

    $last = $remaining | Where-Object { $_.NodeId -ne $second.NodeId }

    Write-Host "Waiting for cluster to detect loss..."
    Start-Sleep -Seconds 3

    Write-Host "Attempting publish with only 1 node alive (should fail closed)..."
    $failed = $false
    try {
        Invoke-Publish @($last) "smoke-below-minisr" 10
    } catch {
        $failed = $true
        Write-Host "OK: publish correctly failed closed: $_"
    }
    if (-not $failed) {
        throw "Publish succeeded with only 1 node alive (expected fail-closed)"
    }

    # --- Restart follower and verify catch-up ---
    Write-Host "Restarting killed follower $($second.NodeId)..."
    $second.Proc = (Start-Node $second.NodeId $second.Index).Proc
    Wait-Healthy $second

    Write-Host "Publishing after follower restart..."
    $active = @($last, $second)
    Invoke-Publish $active "smoke-restart" 10

    if (-not (Test-ReplicationLagZero $active)) {
        throw "Replication lag did not reach zero after follower restart"
    }
    Write-Host "OK: follower caught up and lag is 0"

    Write-Host "`n=== SMOKE TEST PASSED ==="
} finally {
    Write-Host "Cleaning up processes..."
    foreach ($n in $nodes) {
        if ($n.Proc -and -not $n.Proc.HasExited) {
            Stop-Process -Id $n.Proc.Id -Force -ErrorAction SilentlyContinue
        }
    }
}
