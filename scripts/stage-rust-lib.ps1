# Stage the Rust dedup shared library for Windows builds.
# Copies the .dll (and optional .lib/.exp companion files) from the cargo
# output directory into one or more destination directories. If a destination
# file is locked by a running process, it is renamed to <file>.<timestamp>.old
# so the new artifact can be staged without failing the build.
param(
    [Parameter(Mandatory = $true)]
    [string]$SourceDir,

    [Parameter(Mandatory = $true)]
    [string]$BaseName,

    [Parameter(Mandatory = $true)]
    [string]$Destinations
)

$ErrorActionPreference = "Stop"

$srcDll = Join-Path $SourceDir "$BaseName.dll"
if (!(Test-Path $srcDll)) {
    throw "Rust artifact missing: $srcDll"
}

# Get-FileHash is not available in very old Windows PowerShell builds,
# so compute SHA256 directly via .NET to keep this compatible.
function Get-FileHashSha256($path) {
    $stream = $null
    $sha = $null
    try {
        $stream = [System.IO.File]::OpenRead($path)
        $sha = [System.Security.Cryptography.SHA256]::Create()
        $bytes = $sha.ComputeHash($stream)
        return [BitConverter]::ToString($bytes) -replace '-', ''
    }
    finally {
        if ($sha) { $sha.Dispose() }
        if ($stream) { $stream.Dispose() }
    }
}

function Get-LeafPattern($path) {
    $leaf = Split-Path $path -Leaf
    return "$leaf.*.old"
}

function Copy-IfChanged($srcFile, $dstFile) {
    $dstDir = Split-Path $dstFile -Parent
    if ($dstDir -and !(Test-Path $dstDir)) {
        New-Item -ItemType Directory -Force $dstDir | Out-Null
    }

    if (Test-Path $dstFile) {
        $srcHash = Get-FileHashSha256 $srcFile
        $dstHash = Get-FileHashSha256 $dstFile
        if ($srcHash -eq $dstHash) {
            # Already up to date; nothing to do.
            return
        }

        try {
            Copy-Item -Force $srcFile $dstFile
        }
        catch {
            # File is likely locked by a running cronos-api process.
            # Rename it out of the way and retry; the running process keeps
            # the renamed handle, and the next run will pick up the new DLL.
            $oldName = "$dstFile.$((Get-Date).ToString('yyyyMMddHHmmss')).old"
            Get-ChildItem -Path $dstDir -Filter (Get-LeafPattern $dstFile) -ErrorAction SilentlyContinue |
                Remove-Item -Force -ErrorAction SilentlyContinue
            Rename-Item -Path $dstFile -NewName $oldName -ErrorAction SilentlyContinue
            Copy-Item -Force $srcFile $dstFile
        }
    }
    else {
        Copy-Item -Force $srcFile $dstFile
    }
}

$destList = $Destinations -split ";" | ForEach-Object { $_.Trim() } | Where-Object { $_ }
foreach ($d in $destList) {
    Copy-IfChanged $srcDll (Join-Path $d "$BaseName.dll")
}

$srcLib = Join-Path $SourceDir "$BaseName.dll.lib"
if (Test-Path $srcLib) {
    foreach ($d in $destList) {
        Copy-IfChanged $srcLib (Join-Path $d "$BaseName.dll.lib")
    }

    $srcExp = Join-Path $SourceDir "$BaseName.dll.exp"
    if (Test-Path $srcExp) {
        foreach ($d in $destList) {
            Copy-IfChanged $srcExp (Join-Path $d "$BaseName.dll.exp")
        }
    }
}
