param(
    [Parameter(Mandatory = $true)][string]$Binary,
    [Parameter(Mandatory = $true)][string]$Case,
    [string]$DataCase = $Case,
    [int]$EventsPerPublisher = 50000,
    [int]$PublishersPerNode = 32,
    [int]$PartitionCount = 32,
    [int]$BatchSize = 4000,
    [int]$SegmentSize = 134217728,
    [int]$ReplicationFactor = 1,
    [int]$MinInSyncReplicas = 1,
    [bool]$AllowDuplicate = $true
)

$ErrorActionPreference = 'Stop'
$outputRoot = Join-Path (Split-Path -Parent $PSScriptRoot) 'build/perf-comparison'
New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
$runPath = Join-Path $outputRoot $Case
$dataPath = Join-Path $outputRoot $DataCase
New-Item -ItemType Directory -Force -Path $runPath | Out-Null
New-Item -ItemType Directory -Force -Path $dataPath | Out-Null
$freshCluster = -not (Test-Path -LiteralPath (Join-Path $dataPath 'node1/raft'))
$binaryPath = (Resolve-Path -LiteralPath $Binary).Path
$harnessPath = Join-Path $outputRoot 'cluster-loadtest.exe'
$started = @()

try {
    for ($nodeIndex = 1; $nodeIndex -le 3; $nodeIndex++) {
        $grpcPort = 9099 + $nodeIndex
        $httpPort = 8179 + $nodeIndex
        $gossipPort = 8036 + 10 * $nodeIndex
        $clusterPort = $gossipPort + 1
        $raftPort = $gossipPort + 2
        $nodePath = Join-Path $dataPath "node$nodeIndex"
        New-Item -ItemType Directory -Force -Path $nodePath | Out-Null
        $arguments = @(
            '--dev', '--auth-enabled=false', '--cluster',
            "--node-id=node$nodeIndex",
            "--data-dir=$nodePath",
            "--grpc-addr=127.0.0.1:$grpcPort",
            "--http-addr=127.0.0.1:$httpPort",
            "--cluster-gossip-addr=127.0.0.1:$gossipPort",
            "--cluster-grpc-addr=127.0.0.1:$clusterPort",
            "--cluster-raft-addr=127.0.0.1:$raftPort",
            "--partition-count=$PartitionCount",
            "--replication-factor=$ReplicationFactor",
            "--min-insync-replicas=$MinInSyncReplicas",
            '--fsync-mode=periodic', '--flush-interval=100',
            "--segment-size=$SegmentSize", '--tracing-enabled=false'
        )
        if ($nodeIndex -gt 1) {
            $arguments += '--cluster-seeds=127.0.0.1:8046'
        }
        $process = Start-Process -FilePath $binaryPath -ArgumentList $arguments -WorkingDirectory $runPath -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runPath "node$nodeIndex.stdout.log") -RedirectStandardError (Join-Path $runPath "node$nodeIndex.stderr.log")
        $started += $process
        if ($freshCluster) {
            $healthy = $false
            for ($attempt = 0; $attempt -lt 40; $attempt++) {
                if ($process.HasExited) { throw "node$nodeIndex exited with $($process.ExitCode)" }
                try {
                    $response = Invoke-WebRequest -Uri "http://127.0.0.1:$httpPort/health" -UseBasicParsing -TimeoutSec 2
                    if ($response.StatusCode -eq 200) { $healthy = $true; break }
                } catch { }
                Start-Sleep -Milliseconds 500
            }
            if (-not $healthy) { throw "node$nodeIndex failed health check" }
        }
    }

    for ($nodeIndex = 1; $nodeIndex -le 3; $nodeIndex++) {
        $httpPort = 8179 + $nodeIndex
        $process = $started[$nodeIndex - 1]
        $healthy = $false
        for ($attempt = 0; $attempt -lt 40; $attempt++) {
            if ($process.HasExited) { throw "node$nodeIndex exited with $($process.ExitCode)" }
            try {
                $response = Invoke-WebRequest -Uri "http://127.0.0.1:$httpPort/health" -UseBasicParsing -TimeoutSec 2
                if ($response.StatusCode -eq 200) { $healthy = $true; break }
            } catch { }
            Start-Sleep -Milliseconds 500
        }
        if (-not $healthy) { throw "node$nodeIndex failed health check" }
    }

    Start-Sleep -Seconds 8
    $duplicateFlag = if ($AllowDuplicate) { 'true' } else { 'false' }
    $harnessArguments = @(
        '-node1-grpc=127.0.0.1:9100', '-node1-http=127.0.0.1:8180',
        '-node2-grpc=127.0.0.1:9101', '-node2-http=127.0.0.1:8181',
        '-node3-grpc=127.0.0.1:9102', '-node3-http=127.0.0.1:8182',
        '-nodes=3', "-publishers=$PublishersPerNode", "-events=$EventsPerPublisher",
        '-payload=256', '-delay=0', '-topic=cluster-loadtest',
        '-round-robin=true', '-batch', "-batch-size=$BatchSize",
        "-allow-duplicate=$duplicateFlag", "-partition-count=$PartitionCount"
    )
    $outputPath = Join-Path $runPath 'loadtest.txt'
    & $harnessPath @harnessArguments *> $outputPath
    if ($LASTEXITCODE -ne 0) { throw "loadtest exited with $LASTEXITCODE; see $outputPath" }
    $leaderLines = @(Select-String -Path $outputPath -Pattern 'Leader partitions on node[123]: ([0-9]+)')
    if ($leaderLines.Count -ne 3 -or @($leaderLines | Where-Object { [int]$_.Matches[0].Groups[1].Value -eq 0 }).Count -gt 0) {
        throw "invalid three-node leader distribution; see $outputPath"
    }
    foreach ($nodeIndex in 1..3) {
        $httpPort = 8179 + $nodeIndex
        try {
            $metrics = Invoke-WebRequest -Uri "http://127.0.0.1:$httpPort/metrics" -UseBasicParsing -TimeoutSec 5
            Set-Content -LiteralPath (Join-Path $runPath "node$nodeIndex.metrics.txt") -Value $metrics.Content
        } catch { }
    }
    if (-not (Select-String -Path $outputPath -Pattern 'Total Errors:\s+0\s' -Quiet)) {
        throw "loadtest reported failed publishes; see $outputPath"
    }
    Get-Content $outputPath | Select-String -Pattern 'Duration:|Total Published:|Total Errors:|Success Rate:|Throughput:|Performance:'
}
finally {
    foreach ($process in $started) {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
    }
}
