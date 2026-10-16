param(
    [string]$Image = 'cronos-db:hardening-v070-check',
    [ValidateRange(5,60)][int]$Seconds = 15,
    [ValidateRange(1000,1000000)][int]$MaxEvents = 500000,
    [ValidateRange(128,4096)][int]$Payload = 256,
    [string]$Plan = '1:1,1:4,100:1,100:4,100:8,1000:1,1000:4,1000:8,1000:16',
    [ValidateRange(256,384)][int]$NodeMemoryMiB = 256,
    [ValidateRange(1536,16384)][int]$AbortFreeMiB = 1536
)

# Disposable production-mode cluster. Never touches existing containers or data.
# Docker limits bound the servers; this supervisor stops the load before those
# limits are reached and leaves a large reserve for the desktop and other apps.
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$runID = 'cronos-peak-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0,6)
$runDir = Join-Path $repo "build/throughput/$runID"
$privateDir = Join-Path $runDir 'private'
$docker = (Get-Command docker.exe -ErrorAction SilentlyContinue).Source
if (-not $docker) { $docker = 'C:\Program Files\Docker\Docker\resources\bin\docker.exe' }
if (-not (Test-Path -LiteralPath $docker)) { throw 'Docker CLI is unavailable' }
$go = Join-Path $env:USERPROFILE 'go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.9.windows-amd64/bin/go.exe'
if (-not (Test-Path -LiteralPath $go)) { $go = (Get-Command go.exe).Source }
$binary = Join-Path $repo 'build/throughput/throughput.exe'
$containers = @()
$harness = $null
$networkCreated = $false
$guardReason = $null
$minFree = [double]::PositiveInfinity
$maxCPU = 0
$maxContainerPercent = 0
$maxClientMiB = 0
$lastDockerSample = [datetime]::MinValue
$lastDiskSample = [datetime]::MinValue
$cpuHighSamples = 0
$deadline = (Get-Date).AddMinutes(19)
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

function Docker([string[]]$Arguments) {
    $info = [System.Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $docker
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    foreach ($argument in $Arguments) { $info.ArgumentList.Add($argument) }
    $process = [System.Diagnostics.Process]::Start($info)
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit(12000)) { $process.Kill(); throw 'Docker command timed out' }
    $out = $stdout.GetAwaiter().GetResult().Trim()
    $err = $stderr.GetAwaiter().GetResult().Trim()
    if ($process.ExitCode -ne 0) { throw "Docker failed: $err $out" }
    if ($Arguments[0] -eq 'logs') { return ($out + "`n" + $err).Trim() }
    return $out
}

function Guard {
    $os = Get-CimInstance Win32_OperatingSystem
    $free = [math]::Round($os.FreePhysicalMemory / 1024)
    $script:minFree = [math]::Min($script:minFree,$free)
    $cpu = [int](Get-CimInstance Win32_Processor | Measure-Object LoadPercentage -Average).Average
    $script:maxCPU = [math]::Max($script:maxCPU,$cpu)
    if ($cpu -gt 90) { $script:cpuHighSamples++ } else { $script:cpuHighSamples = 0 }
    if ($free -lt $AbortFreeMiB) { throw "available host memory $free MiB is below reserve $AbortFreeMiB MiB" }
    if ($script:cpuHighSamples -ge 3) { throw "host CPU exceeded 90% for three consecutive samples" }
    if ((Get-Date) -gt $deadline) { throw '19-minute total time limit reached' }
    foreach ($drive in @('C','D')) {
        $volume = Get-PSDrive -Name $drive -ErrorAction SilentlyContinue
        if ($volume -and $volume.Free -lt 20GB) { throw "drive $drive has less than 20 GiB free" }
    }
    if ($script:harness -and -not $script:harness.HasExited) {
        $script:harness.Refresh()
        $working = $script:harness.WorkingSet64 / 1MB
        $script:maxClientMiB = [math]::Max($script:maxClientMiB,$working)
        if ($working -gt 192) { throw "load generator reached $([int]$working) MiB (192 MiB ceiling)" }
    }
    if ($containers.Count -gt 0 -and ((Get-Date)-$script:lastDockerSample).TotalSeconds -ge 4) {
        $stats = Docker (@('stats','--no-stream','--format','{{json .}}') + $containers)
        foreach ($line in ($stats -split "`n")) {
            if (-not $line) { continue }
            $row = $line | ConvertFrom-Json
            $percent = [double]::Parse($row.MemPerc.TrimEnd('%'),[cultureinfo]::InvariantCulture)
            $script:maxContainerPercent = [math]::Max($script:maxContainerPercent,$percent)
            if ($percent -gt 75) { throw "$($row.Name) reached $percent% of its memory cap (75% stop threshold)" }
        }
        $script:lastDockerSample = Get-Date
    }
    if ($containers.Count -gt 0 -and ((Get-Date)-$script:lastDiskSample).TotalSeconds -ge 10) {
        foreach ($name in $containers) {
            $usage = Docker @('exec',$name,'du','-sb','/data')
            if ([long]($usage -split '\s+')[0] -gt 512MB) { throw "$name data exceeded 512 MiB" }
        }
        $script:lastDiskSample = Get-Date
    }
    [pscustomobject]@{time=(Get-Date -Format o);host_free_mib=$free;host_cpu_percent=$cpu;client_peak_mib=[math]::Round($maxClientMiB,1);node_peak_percent=$maxContainerPercent} |
        Export-Csv -Path (Join-Path $runDir 'resources.csv') -Append -NoTypeInformation
}

$before = Get-CimInstance Win32_OperatingSystem
$minimum = [math]::Max(2560,$AbortFreeMiB + 3*$NodeMemoryMiB + 256)
if ($before.FreePhysicalMemory/1024 -lt $minimum) {
    $reason="Need $minimum MiB free before starting; currently $([int]($before.FreePhysicalMemory/1024)) MiB. No load started."
    [ordered]@{state='not_started';reason=$reason;host_free_mib=[int]($before.FreePhysicalMemory/1024);required_free_mib=$minimum;containers_started=0} | ConvertTo-Json | Set-Content (Join-Path $runDir 'safety.json')
    throw $reason
}
if ((Get-CimInstance Win32_Processor | Measure-Object NumberOfLogicalProcessors -Sum).Sum -lt 8) { throw 'Default CPU budget requires at least 8 logical processors' }
Guard
$env:CGO_ENABLED='0'; $env:GOTOOLCHAIN='local'; $env:GOARCH='amd64'; $env:GOMAXPROCS='2'; $env:GOMEMLIMIT='192MiB'
Push-Location $repo
try { & $go build -p 1 -o $binary ./tools/throughput; if ($LASTEXITCODE -ne 0) { throw 'Load generator build failed' } }
finally { Pop-Location }
$names = @(1..3 | ForEach-Object { "$runID-node$_" })
& $binary -mode=init "-dir=$privateDir" "-names=$($names -join ',')"
if ($LASTEXITCODE -ne 0) { throw 'Credential setup failed' }

try {
    $imageInfo = Docker @('image','inspect',$Image,'--format','{{.Id}}')
    $hostInfo = [ordered]@{started=(Get-Date -Format o);image=$Image;image_id=$imageInfo;commit=(git -C $repo rev-parse HEAD);node_memory_mib=$NodeMemoryMiB;node_cpus=1;generator_gomaxprocs=2;abort_host_free_mib=$AbortFreeMiB;payload_bytes=$Payload;rf=3;min_isr=2;fsync='batch';dedup=$true;public_tls=$true;replication_mtls=$true;auth=$true;encryption=$true;partitions=3;plan=$Plan;seconds_per_trial=$Seconds;max_events_per_trial=$MaxEvents}
    $hostInfo | ConvertTo-Json -Depth 4 | Set-Content (Join-Path $runDir 'config.json')
    Docker @('network','create',$runID) | Out-Null; $networkCreated=$true
    $ports = @()
    foreach ($i in 1..6) { $listener=[net.sockets.TcpListener]::new([net.ipaddress]::Loopback,0);$listener.Start();$ports+=$listener.LocalEndpoint.Port;$listener.Stop() }
    $seeds = ($names | ForEach-Object { "${_}:7946" }) -join ','
    for ($index=0;$index -lt 3;$index++) {
        Guard
        $name=$names[$index]
        $server=@("--node-id=node$($index+1)",'--cluster','--cluster-expected-nodes=3',"--cluster-seeds=$seeds",'--partition-count=3','--replication-factor=3','--min-insync-replicas=2','--fsync-mode=batch','--flush-interval=2','--segment-size=16777216','--bloom-capacity=1000000','--compaction-interval=2s','--backup-interval=0','--scheduler-memory=33554432','--max-memory-percent=65','--memory-check-interval=250','--max-ready-queue=30000','--max-in-flight=10000','--grpc-addr=0.0.0.0:9000','--http-addr=0.0.0.0:8080',"--cluster-gossip-addr=${name}:7946","--cluster-grpc-addr=${name}:7947","--cluster-raft-addr=${name}:7948",'--tls-enabled','--tls-ca-file=/config/ca.crt','--tls-cert-file=/config/tls.crt','--tls-key-file=/config/tls.key','--http-tls-enabled','--auth-enabled','--auth-policy-file=/config/policy.json','--replication-tls-enabled','--replication-tls-ca-file=/config/ca.crt','--replication-tls-cert-file=/config/tls.crt','--replication-tls-key-file=/config/tls.key','--encryption-enabled','--encryption-key-file=/tmp/master.key')
        if ($index -eq 0) { $server+='--cluster-bootstrap' }
        $entry='umask 077; cp /config/master.key /tmp/master.key; chmod 600 /tmp/master.key; exec /app/cronos-api "$@"'
        $dockerArgs=@('run','-d','--name',$name,'--network',$runID,'--cpus=1',"--memory=$($NodeMemoryMiB)m","--memory-swap=$($NodeMemoryMiB)m",'--pids-limit=128','--log-opt=max-size=5m','--log-opt=max-file=2','--env-file',"$privateDir/server.env",'-e','GOMEMLIMIT=150MiB','--mount',"type=bind,source=$privateDir,target=/config,readonly",'-p',"127.0.0.1:$($ports[$index*2]):9000",'-p',"127.0.0.1:$($ports[$index*2+1]):8080",'--entrypoint','/bin/sh',$Image,'-ec',$entry,'peak')+$server
        $containers += $name # Also clean a container whose creation only partly succeeds.
        Docker $dockerArgs | Out-Null
    }
    $readyDeadline=(Get-Date).AddSeconds(100)
    do {
        Guard
        $ready=0
        for ($index=0;$index -lt 3;$index++) { try { $response=Invoke-WebRequest -Uri "https://127.0.0.1:$($ports[$index*2+1])/health/ready" -SkipCertificateCheck -TimeoutSec 2;if($response.StatusCode -eq 200){$ready++} } catch {} }
        if($ready -lt 3){Start-Sleep -Seconds 1}
    } while($ready -lt 3 -and (Get-Date) -lt $readyDeadline)
    if($ready -lt 3){throw 'Cluster did not become ready'}
    # Leave time for the assigned replicas to connect before measurement.
    foreach($i in 1..3){Guard;Start-Sleep -Seconds 1}
    $nodeMap = @(0..2 | ForEach-Object { "node$($_+1)=127.0.0.1:$($ports[$_*2])" }) -join ','
    $clientArgs=@("-dir=$privateDir","-nodes=$nodeMap","-seconds=$Seconds","-max-events=$MaxEvents","-payload=$Payload","-plan=$Plan")
    $env:GOMEMLIMIT='96MiB'
    $harness=Start-Process -FilePath $binary -ArgumentList $clientArgs -WorkingDirectory $repo -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runDir 'trials.jsonl') -RedirectStandardError (Join-Path $runDir 'client.log')
    $harness.PriorityClass='BelowNormal'
    while(-not $harness.HasExited){Guard;Start-Sleep -Milliseconds 500}
    $harness.WaitForExit()
    if($harness.ExitCode -ne 0){throw "Load generator exited $($harness.ExitCode); inspect client.log"}
}
catch {
    $guardReason=$_.Exception.Message
    Write-Warning "Stopped safely: $guardReason"
    if(Test-Path -LiteralPath $privateDir){Set-Content -LiteralPath (Join-Path $privateDir 'STOP') -Value $guardReason}
    if($harness -and -not $harness.HasExited){if(-not $harness.WaitForExit(5000)){Stop-Process -Id $harness.Id -Force -ErrorAction SilentlyContinue}}
}
finally {
    if(Test-Path (Join-Path $privateDir 'results.json')){Copy-Item -LiteralPath (Join-Path $privateDir 'results.json') -Destination (Join-Path $runDir 'results.json')}
    foreach($name in $containers){
        try{Docker @('logs','--tail','200',$name) | Set-Content (Join-Path $runDir "$name.log")}catch{}
        try{Docker @('inspect',$name,'--format','{{json .State}}') | Set-Content (Join-Path $runDir "$name.state.json")}catch{}
        try{Docker @('exec',$name,'du','-sb','/data') | Set-Content (Join-Path $runDir "$name.data.txt")}catch{}
        try{Docker @('rm','-f','-v',$name) | Out-Null}catch{Write-Warning $_.Exception.Message}
    }
    if($networkCreated){try{Docker @('network','rm',$runID) | Out-Null}catch{Write-Warning $_.Exception.Message}}
    # Verify this absolute path is the private child of this run before deleting.
    $resolvedPrivate=[io.path]::GetFullPath($privateDir)
    $expectedPrivate=[io.path]::GetFullPath((Join-Path $runDir 'private'))
    if($resolvedPrivate -ne $expectedPrivate -or -not $resolvedPrivate.StartsWith([io.path]::GetFullPath((Join-Path $repo 'build/throughput'))+[io.path]::DirectorySeparatorChar)){throw 'Refusing credential cleanup outside throughput run'}
    if(Test-Path -LiteralPath $resolvedPrivate){Remove-Item -LiteralPath $resolvedPrivate -Recurse -Force}
    [ordered]@{finished=(Get-Date -Format o);guard_reason=$guardReason;min_host_free_mib=$minFree;max_host_cpu_percent=$maxCPU;max_node_memory_percent=$maxContainerPercent;max_generator_mib=[math]::Round($maxClientMiB,1)} | ConvertTo-Json | Set-Content (Join-Path $runDir 'safety.json')
}
Write-Output "Evidence: $runDir"
if(Test-Path (Join-Path $runDir 'results.json')){Get-Content (Join-Path $runDir 'results.json') -Raw}
