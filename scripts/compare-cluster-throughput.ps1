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
    [bool]$AllowDuplicate = $true,
    # Memory guard: the three nodes and the load generator share this host.
    # The run does not start below MinFreeMB and is stopped below AbortFreeMB.
    [int]$MinFreeMB = 1200,
    [int]$AbortFreeMB = 350,
    # Delete the node data directories afterwards (each run leaves gigabytes).
    [switch]$RemoveData,
    # Start each node with --pprof-addr (127.0.0.1:6061-6063) and save a CPU
    # profile taken during the load plus block and mutex profiles after it.
    [switch]$Pprof,
    [int]$CpuSeconds = 6,
    # Wait before scraping /metrics, e.g. to let followers finish catching up.
    [int]$SettleSeconds = 0
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

function Get-FreeMB { [int]((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory / 1024) }

function Wait-NodeHealthy([System.Diagnostics.Process]$Process, [int]$NodeIndex, [int]$HttpPort) {
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        if ($Process.HasExited) { throw "node$NodeIndex exited with $($Process.ExitCode)" }
        try {
            $response = Invoke-WebRequest -Uri "http://127.0.0.1:$HttpPort/health" -UseBasicParsing -TimeoutSec 2
            if ($response.StatusCode -eq 200) { return }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    throw "node$NodeIndex failed health check"
}

$free = Get-FreeMB
if ($free -lt $MinFreeMB) { throw "only $free MB of memory available (< $MinFreeMB); not starting" }

try {
    # A new cluster starts each node after the previous one is healthy. Nodes of
    # an existing cluster need each other for Raft quorum, so they start together.
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
            '--cluster-expected-nodes=3',
            '--fsync-mode=periodic', '--flush-interval=100',
            "--segment-size=$SegmentSize", '--tracing-enabled=false'
        )
        if ($nodeIndex -gt 1) {
            $arguments += '--cluster-seeds=127.0.0.1:8046'
        }
        if ($Pprof) {
            $arguments += "--pprof-addr=127.0.0.1:$(6060 + $nodeIndex)"
        }
        $process = Start-Process -FilePath $binaryPath -ArgumentList $arguments -WorkingDirectory $runPath -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runPath "node$nodeIndex.stdout.log") -RedirectStandardError (Join-Path $runPath "node$nodeIndex.stderr.log")
        $started += $process
        if ($freshCluster) { Wait-NodeHealthy $process $nodeIndex $httpPort }
    }
    for ($nodeIndex = 1; $nodeIndex -le 3; $nodeIndex++) {
        Wait-NodeHealthy $started[$nodeIndex - 1] $nodeIndex (8179 + $nodeIndex)
    }

    # Healthy only means the process answers. Publishes are served once every
    # partition has a leader that has loaded it, which /health/ready reports;
    # a new cluster assigns leaders a few seconds after its last node joined.
    # Builds that predate that check answer ready at once, hence the pause.
    Start-Sleep -Seconds 8
    $readyDeadline = (Get-Date).AddSeconds(90)
    do {
        $notReady = @(1..3 | Where-Object {
            try { (Invoke-WebRequest -Uri "http://127.0.0.1:$(8179 + $_)/health/ready" -UseBasicParsing -TimeoutSec 2).StatusCode -ne 200 } catch { $true }
        })
        if ($notReady.Count -gt 0) { Start-Sleep -Milliseconds 500 }
    } while ($notReady.Count -gt 0 -and (Get-Date) -lt $readyDeadline)
    if ($notReady.Count -gt 0) { throw "nodes $($notReady -join ', ') did not become ready to serve publishes" }
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
    $logPath = Join-Path $runPath 'loadtest.log.txt'

    $cpuProfiles = @()
    if ($Pprof) {
        foreach ($nodeIndex in 1..3) {
            $cpuProfiles += Start-Process -FilePath 'curl.exe' -WindowStyle Hidden -PassThru -ArgumentList @('-s', '-o', (Join-Path $runPath "node$nodeIndex.cpu.pb.gz"), "http://127.0.0.1:$(6060 + $nodeIndex)/debug/pprof/profile?seconds=$CpuSeconds")
        }
        Start-Sleep -Milliseconds 300
    }

    $harness = Start-Process -FilePath $harnessPath -ArgumentList $harnessArguments -WindowStyle Hidden -PassThru -RedirectStandardOutput $outputPath -RedirectStandardError $logPath
    $minFree = Get-FreeMB
    while (-not $harness.HasExited) {
        Start-Sleep -Milliseconds 400
        $now = Get-FreeMB
        if ($now -lt $minFree) { $minFree = $now }
        if ($now -lt $AbortFreeMB) {
            Stop-Process -Id $harness.Id -Force -ErrorAction SilentlyContinue
            throw "aborted: available memory fell to $now MB (< $AbortFreeMB)"
        }
    }
    $harness.WaitForExit()
    if ($harness.ExitCode -ne 0) { throw "loadtest exited with $($harness.ExitCode); see $logPath" }

    if ($Pprof) {
        foreach ($job in $cpuProfiles) { $null = $job.WaitForExit(($CpuSeconds + 10) * 1000) }
        foreach ($nodeIndex in 1..3) {
            foreach ($kind in 'block', 'mutex') {
                & curl.exe -s -o (Join-Path $runPath "node$nodeIndex.$kind.pb.gz") "http://127.0.0.1:$(6060 + $nodeIndex)/debug/pprof/$kind"
            }
        }
    }

    $leaderLines = @(Select-String -Path $logPath -Pattern 'Leader partitions on node[123]: ([0-9]+)')
    if ($leaderLines.Count -ne 3 -or @($leaderLines | Where-Object { [int]$_.Matches[0].Groups[1].Value -eq 0 }).Count -gt 0) {
        throw "invalid three-node leader distribution; see $logPath"
    }
    if ($SettleSeconds -gt 0) { Start-Sleep -Seconds $SettleSeconds }
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
    "Lowest available memory during the load: $minFree MB"
}
finally {
    foreach ($process in $started) {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        }
    }
    if ($RemoveData) {
        Start-Sleep -Milliseconds 800
        foreach ($nodeIndex in 1..3) {
            $nodePath = Join-Path $dataPath "node$nodeIndex"
            if (Test-Path -LiteralPath $nodePath) {
                Remove-Item -LiteralPath $nodePath -Recurse -Force -ErrorAction SilentlyContinue
            }
        }
    }
}
