param(
    [ValidateRange(5,60)][int]$Seconds = 15,
    [ValidateRange(1000,1000000)][int]$MaxEvents = 500000,
    [ValidateRange(128,4096)][int]$Payload = 256,
    [string]$Plan = '1:1,1:4,100:1,100:4,100:8,1000:1,1000:4,1000:8,1000:16',
    [ValidateRange(256,512)][int]$NodeMemoryMiB = 384,
    [ValidateSet(1,2)][int]$NodeCPUs = 2,
    [ValidateRange(1536,16384)][int]$AbortFreeMiB = 1536,
    # A server built elsewhere, e.g. from an earlier release for a matched
    # comparison. Empty builds ./cmd/api from this checkout.
    [string]$ServerBinary = ''
)

# The same measurement as measure-throughput.ps1, with the three nodes as plain
# Windows processes instead of containers, for when Docker is not running.
#
# There is no cgroup to cap a process here, so this supervisor is the cap: it
# samples every node's memory four times a second and stops everything at 75%
# of -NodeMemoryMiB, and each node is told that budget (--memory-limit) so it
# refuses publishes at 65% of it on its own. GOMEMLIMIT and GOMAXPROCS bound
# the Go runtime, the nodes and the load generator run below normal priority,
# and the host reserve and time limit are those of the container runner.
#
# Durability on Windows (FlushFileBuffers through NTFS) is not what it is on
# Linux, so the numbers describe this machine, not a Linux server.
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$runID = 'cronos-native-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0,6)
$runDir = Join-Path $repo "build/throughput/$runID"
$privateDir = Join-Path $runDir 'private'
$go = Join-Path $env:USERPROFILE 'go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.9.windows-amd64/bin/go.exe'
if (-not (Test-Path -LiteralPath $go)) { throw 'The amd64 Go toolchain is missing' }
$harnessBinary = Join-Path $repo 'build/throughput/throughput.exe'
$serverBinary = if ($ServerBinary) { (Resolve-Path -LiteralPath $ServerBinary).Path } else { Join-Path $repo 'build/throughput/cronos-api.exe' }
$nodes = @()
$harness = $null
$guardReason = $null
$minFree = [double]::PositiveInfinity
$maxCPU = 0
$maxNodeMiB = 0
$maxClientMiB = 0
$cpuHighSamples = 0
$lastDiskSample = [datetime]::MinValue
$deadline = (Get-Date).AddMinutes(19)
New-Item -ItemType Directory -Force -Path $privateDir | Out-Null

function Guard {
    $os = Get-CimInstance Win32_OperatingSystem
    $free = [math]::Round($os.FreePhysicalMemory / 1024)
    $script:minFree = [math]::Min($script:minFree,$free)
    $cpu = [int](Get-CimInstance Win32_Processor | Measure-Object LoadPercentage -Average).Average
    $script:maxCPU = [math]::Max($script:maxCPU,$cpu)
    if ($cpu -gt 90) { $script:cpuHighSamples++ } else { $script:cpuHighSamples = 0 }
    if ($free -lt $AbortFreeMiB) { throw "available host memory $free MiB is below reserve $AbortFreeMiB MiB" }
    if ($script:cpuHighSamples -ge 3) { throw 'host CPU exceeded 90% for three consecutive samples' }
    if ((Get-Date) -gt $deadline) { throw '19-minute total time limit reached' }
    foreach ($node in $script:nodes) {
        if ($node.Process.HasExited) { throw "$($node.Name) exited with $($node.Process.ExitCode); see its log" }
        $node.Process.Refresh()
        $mib = $node.Process.WorkingSet64 / 1MB
        $script:maxNodeMiB = [math]::Max($script:maxNodeMiB,$mib)
        if ($mib -gt 0.75 * $NodeMemoryMiB) { throw "$($node.Name) reached $([int]$mib) MiB (75% of its $NodeMemoryMiB MiB budget)" }
    }
    if ($script:harness -and -not $script:harness.HasExited) {
        $script:harness.Refresh()
        $working = $script:harness.WorkingSet64 / 1MB
        $script:maxClientMiB = [math]::Max($script:maxClientMiB,$working)
        if ($working -gt 192) { throw "load generator reached $([int]$working) MiB (192 MiB ceiling)" }
    }
    if ($script:nodes.Count -gt 0 -and ((Get-Date)-$script:lastDiskSample).TotalSeconds -ge 5) {
        foreach ($node in $script:nodes) {
            $bytes = (Get-ChildItem -LiteralPath $node.Data -Recurse -File -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum
            if ($bytes -gt 512MB) { throw "$($node.Name) data exceeded 512 MiB" }
        }
        $drive = Get-PSDrive -Name ($repo.Substring(0,1)) -ErrorAction SilentlyContinue
        if ($drive -and $drive.Free -lt 20GB) { throw "drive $($drive.Name) has less than 20 GiB free" }
        $script:lastDiskSample = Get-Date
    }
    [pscustomobject]@{time=(Get-Date -Format o);host_free_mib=$free;host_cpu_percent=$cpu;node_peak_mib=[math]::Round($maxNodeMiB,1);client_peak_mib=[math]::Round($maxClientMiB,1)} |
        Export-Csv -Path (Join-Path $runDir 'resources.csv') -Append -NoTypeInformation
}

function FreePort {
    $listener = [net.sockets.TcpListener]::new([net.ipaddress]::Loopback,0)
    $listener.Start(); $port = $listener.LocalEndpoint.Port; $listener.Stop()
    return $port
}

$before = Get-CimInstance Win32_OperatingSystem
$minimum = [math]::Max(2560,$AbortFreeMiB + 3*$NodeMemoryMiB + 256)
if ($before.FreePhysicalMemory/1024 -lt $minimum) {
    $reason = "Need $minimum MiB free before starting; currently $([int]($before.FreePhysicalMemory/1024)) MiB. No load started."
    [ordered]@{state='not_started';reason=$reason} | ConvertTo-Json | Set-Content (Join-Path $runDir 'safety.json')
    throw $reason
}
if ((Get-CimInstance Win32_Processor | Measure-Object NumberOfLogicalProcessors -Sum).Sum -lt 8*$NodeCPUs) { throw 'CPU budget requires at least eight logical processors per node CPU' }
Guard

$env:CGO_ENABLED='0'; $env:GOTOOLCHAIN='local'; $env:GOARCH='amd64'; $env:GOMAXPROCS='2'; $env:GOMEMLIMIT='192MiB'
Push-Location $repo
try {
    & $go build -p 1 -o $harnessBinary ./tools/throughput; if ($LASTEXITCODE -ne 0) { throw 'Load generator build failed' }
    if (-not $ServerBinary) { & $go build -p 1 -o $serverBinary ./cmd/api; if ($LASTEXITCODE -ne 0) { throw 'Server build failed' } }
}
finally { Pop-Location }
$names = @('node1','node2','node3')
& $harnessBinary -mode=init "-dir=$privateDir" "-names=$($names -join ',')"
if ($LASTEXITCODE -ne 0) { throw 'Credential setup failed' }
# Servers before v0.7.0 have no scheduler memory budget; they would refuse the flag.
$serverHelp = (& $serverBinary -h 2>&1) -join "`n"
$schedulerMemory = if ($serverHelp -match 'scheduler-memory') { @('--scheduler-memory=33554432') } else { @() }
$secret = ((Get-Content (Join-Path $privateDir 'server.env')) | Where-Object { $_ -like 'CRONOS_AUTH_JWT_SECRET=*' }) -replace '^CRONOS_AUTH_JWT_SECRET=',''

try {
    [ordered]@{started=(Get-Date -Format o);runner='native-windows';commit=(git -C $repo rev-parse HEAD);server_binary=$serverBinary;server_sha256=(Get-FileHash -LiteralPath $serverBinary).Hash;node_memory_mib=$NodeMemoryMiB;node_cpus=$NodeCPUs;abort_host_free_mib=$AbortFreeMiB;payload_bytes=$Payload;rf=3;min_isr=2;fsync='batch';dedup=$true;public_tls=$true;replication_mtls=$true;auth=$true;encryption=$true;partitions=3;plan=$Plan;seconds_per_trial=$Seconds;max_events_per_trial=$MaxEvents} |
        ConvertTo-Json | Set-Content (Join-Path $runDir 'config.json')
    $ports = @{}
    foreach ($name in $names) { $ports[$name] = @{grpc=FreePort;http=FreePort;gossip=FreePort;cluster=FreePort;raft=FreePort} }
    $seeds = ($names | ForEach-Object { "127.0.0.1:$($ports[$_].gossip)" }) -join ','
    $config = $privateDir
    foreach ($name in $names) {
        Guard
        $p = $ports[$name]
        $data = Join-Path $runDir "$name-data"
        New-Item -ItemType Directory -Force -Path $data | Out-Null
        $server = @("--node-id=$name",'--cluster','--cluster-expected-nodes=3',"--cluster-seeds=$seeds",'--partition-count=3','--replication-factor=3','--min-insync-replicas=2','--fsync-mode=batch','--flush-interval=2','--segment-size=16777216','--bloom-capacity=1000000','--compaction-interval=2s','--backup-interval=0','--max-memory-percent=65','--memory-check-interval=250','--max-ready-queue=30000','--max-in-flight=10000',
            "--memory-limit=$($NodeMemoryMiB*1MB)","--data-dir=$data","--raft-dir=$data/raft","--grpc-addr=127.0.0.1:$($p.grpc)","--http-addr=127.0.0.1:$($p.http)","--cluster-gossip-addr=127.0.0.1:$($p.gossip)","--cluster-grpc-addr=127.0.0.1:$($p.cluster)","--cluster-raft-addr=127.0.0.1:$($p.raft)",
            '--tls-enabled',"--tls-ca-file=$config/ca.crt","--tls-cert-file=$config/tls.crt","--tls-key-file=$config/tls.key",'--http-tls-enabled','--auth-enabled',"--auth-policy-file=$config/policy.json",
            '--replication-tls-enabled',"--replication-tls-ca-file=$config/ca.crt","--replication-tls-cert-file=$config/tls.crt","--replication-tls-key-file=$config/tls.key",'--encryption-enabled',"--encryption-key-file=$config/master.key")
        $server += $schedulerMemory
        if ($name -eq 'node1') { $server += '--cluster-bootstrap' }
        $env:CRONOS_AUTH_JWT_SECRET = $secret; $env:GOMEMLIMIT = '150MiB'; $env:GOMAXPROCS = "$NodeCPUs"
        $process = Start-Process -FilePath $serverBinary -ArgumentList $server -WorkingDirectory $data -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $runDir "$name.log") -RedirectStandardError (Join-Path $runDir "$name.err.log")
        $nodes += [pscustomobject]@{Name=$name;Process=$process;Data=$data;Http=$p.http}
        $process.PriorityClass = 'BelowNormal'
    }
    Remove-Item Env:CRONOS_AUTH_JWT_SECRET
    $readyDeadline = (Get-Date).AddSeconds(100)
    do {
        Guard
        $ready = 0
        foreach ($node in $nodes) { try { $r = Invoke-WebRequest -Uri "https://127.0.0.1:$($node.Http)/health/ready" -SkipCertificateCheck -TimeoutSec 2; if ($r.StatusCode -eq 200) { $ready++ } } catch {} }
        if ($ready -lt 3) { Start-Sleep -Seconds 1 }
    } while ($ready -lt 3 -and (Get-Date) -lt $readyDeadline)
    if ($ready -lt 3) { throw 'Cluster did not become ready' }
    foreach ($i in 1..3) { Guard; Start-Sleep -Seconds 1 }
    $nodeMap = ($names | ForEach-Object { "$_=127.0.0.1:$($ports[$_].grpc)" }) -join ','
    $clientArgs = @("-dir=$privateDir","-nodes=$nodeMap","-seconds=$Seconds","-max-events=$MaxEvents","-payload=$Payload","-plan=$Plan")
    $env:GOMEMLIMIT = '96MiB'; $env:GOMAXPROCS = '2'
    $harness = Start-Process -FilePath $harnessBinary -ArgumentList $clientArgs -WorkingDirectory $repo -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $runDir 'trials.jsonl') -RedirectStandardError (Join-Path $runDir 'client.log')
    $harness.PriorityClass = 'BelowNormal'
    while (-not $harness.HasExited) { Guard; Start-Sleep -Milliseconds 250 }
    $harness.WaitForExit()
    if ($harness.ExitCode -ne 0) { throw "Load generator exited $($harness.ExitCode); inspect client.log" }
}
catch {
    $guardReason = $_.Exception.Message
    Write-Warning "Stopped safely: $guardReason"
    if (Test-Path -LiteralPath $privateDir) { Set-Content -LiteralPath (Join-Path $privateDir 'STOP') -Value $guardReason }
    if ($harness -and -not $harness.HasExited) { if (-not $harness.WaitForExit(5000)) { Stop-Process -Id $harness.Id -Force -ErrorAction SilentlyContinue } }
}
finally {
    if (Test-Path (Join-Path $privateDir 'results.json')) { Copy-Item -LiteralPath (Join-Path $privateDir 'results.json') -Destination (Join-Path $runDir 'results.json') }
    foreach ($node in $nodes) {
        try { Invoke-WebRequest -Uri "https://127.0.0.1:$($node.Http)/metrics" -SkipCertificateCheck -TimeoutSec 3 | Select-Object -ExpandProperty Content | Set-Content (Join-Path $runDir "$($node.Name).metrics.txt") } catch {}
        if (-not $node.Process.HasExited) { Stop-Process -Id $node.Process.Id -Force -ErrorAction SilentlyContinue }
    }
    foreach ($node in $nodes) { try { $node.Process.WaitForExit(10000) | Out-Null } catch {} }
    # Delete the credentials and the test data only inside this run's directory.
    $root = [io.path]::GetFullPath((Join-Path $repo 'build/throughput')) + [io.path]::DirectorySeparatorChar
    foreach ($path in @($privateDir) + @($nodes | ForEach-Object { $_.Data })) {
        $full = [io.path]::GetFullPath($path)
        if ($full.StartsWith($root + $runID + [io.path]::DirectorySeparatorChar)) { Remove-Item -LiteralPath $full -Recurse -Force -ErrorAction SilentlyContinue }
    }
    [ordered]@{finished=(Get-Date -Format o);guard_reason=$guardReason;min_host_free_mib=$minFree;max_host_cpu_percent=$maxCPU;max_node_mib=[math]::Round($maxNodeMiB,1);max_generator_mib=$maxClientMiB} |
        ConvertTo-Json | Set-Content (Join-Path $runDir 'safety.json')
    Write-Output "Run directory: $runDir"
    if (Test-Path (Join-Path $runDir 'results.json')) { Get-Content (Join-Path $runDir 'results.json') }
}
