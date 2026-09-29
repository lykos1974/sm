[CmdletBinding()]
param(
    [ValidateSet('Validate', 'Apply', 'Rollback')][string]$Mode = 'Validate',
    [string]$SourceRoot,
    [string]$TargetRoot,
    [string]$BackupPath,
    [switch]$ConfirmServicesStopped
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2.0
$Expected = [ordered]@{
    'collector.py' = 'a06f1d0b42e69d70d6b1b21f173e7dbad69941824078332360d044a7b56b7c66'
    'storage.py' = 'de71b146c3f1f0ed3ad630be7dd340c62b505e628bcf0d0d84b463c136f3f26f'
}

function Hash-File([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Canonical-Bytes([string]$Path) {
    $utf8 = [Text.UTF8Encoding]::new($false, $true)
    $content = $utf8.GetString([IO.File]::ReadAllBytes($Path))
    if ($content.Contains("`r") -and -not $content.Contains("`r`n")) { throw 'Unsupported line endings.' }
    $normalized = $content.Replace("`r`n", "`n")
    if ($normalized.Contains("`r")) { throw 'Unsupported line endings.' }
    return ,$utf8.GetBytes($normalized)
}

function Hash-Bytes([byte[]]$Bytes) {
    $hasher = [Security.Cryptography.SHA256]::Create()
    try { return ([BitConverter]::ToString($hasher.ComputeHash($Bytes))).Replace('-', '').ToLowerInvariant() }
    finally { $hasher.Dispose() }
}

function Assert-RegularFile([string]$Path) {
    $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
    if ($item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw "Unsafe file: $Path"
    }
}

function Assert-Package([string]$Root) {
    foreach ($entry in $Expected.GetEnumerator()) {
        $path = Join-Path $Root $entry.Key
        Assert-RegularFile $path
        if ((Hash-Bytes (Canonical-Bytes $path)) -ne $entry.Value) { throw "Package hash mismatch: $($entry.Key)" }
    }
}

function Replace-File([string]$Staged, [string]$Destination, [string]$BackupDirectory) {
    # Windows PowerShell/.NET may reject a null backup path for File.Replace.
    $displaced = Join-Path $BackupDirectory ('displaced_' + [guid]::NewGuid().ToString('N'))
    [IO.File]::Replace($Staged, $Destination, $displaced)
    try { [IO.File]::Delete($displaced) } catch { } # A leftover remains inside the backup only.
}

function Assert-Idle {
    if (-not $ConfirmServicesStopped) { throw 'Stop collector and scanner first; pass -ConfirmServicesStopped.' }
    if (Get-Command Get-CimInstance -ErrorAction SilentlyContinue) {
        $active = @(Get-CimInstance Win32_Process | Where-Object {
            $_.Name -match '^(python|pythonw)(\.exe)?$' -and
            $_.CommandLine -match 'market_collector[\\/](collector|app)\.py'
        })
        if ($active.Count -gt 0) { throw 'Collector process is running.' }
    }
}

if (-not $TargetRoot) { throw 'TargetRoot is required.' }
$target = [IO.Path]::GetFullPath($TargetRoot)
if (-not (Test-Path -LiteralPath $target -PathType Container)) { throw 'Target directory missing.' }

if ($Mode -ne 'Rollback') {
    if (-not $SourceRoot) { throw 'SourceRoot is required.' }
    $source = [IO.Path]::GetFullPath($SourceRoot)
    if ($source -eq $target) { throw 'Source and target must differ.' }
    Assert-Package $source
}

if ($Mode -eq 'Validate') { Write-Output 'PACKAGE_VALID'; return }
Assert-Idle

if ($Mode -eq 'Apply') {
    $before = [ordered]@{}
    foreach ($name in $Expected.Keys) {
        $path = Join-Path $target $name
        Assert-RegularFile $path
        $before[$name] = Hash-File $path
    }
    $backupRoot = Join-Path $target '_collector_gap_backups'
    if (Test-Path -LiteralPath $backupRoot) {
        $rootItem = Get-Item -LiteralPath $backupRoot -Force
        if (-not $rootItem.PSIsContainer -or ($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'Unsafe backup directory.' }
    }
    $backup = Join-Path $backupRoot ((Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmmss_fffffff') + '_' + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $backup -ErrorAction Stop | Out-Null
    try {
        foreach ($name in $Expected.Keys) {
            [IO.File]::Copy((Join-Path $target $name), (Join-Path $backup $name), $false)
            if ((Hash-File (Join-Path $backup $name)) -ne $before[$name]) { throw 'Backup verification failed.' }
        }
        $manifest = [ordered]@{ version = 1; target = $target; original = $before; package = $Expected }
        $manifestText = ConvertTo-Json -InputObject $manifest -Depth 5
        [IO.File]::WriteAllText((Join-Path $backup 'manifest.json'), $manifestText, [Text.UTF8Encoding]::new($false))
        foreach ($name in $Expected.Keys) {
            $staged = Join-Path $backup ("package_" + $name)
            [IO.File]::WriteAllBytes($staged, (Canonical-Bytes (Join-Path $source $name)))
            if ((Hash-File $staged) -ne $Expected[$name]) { throw "Staged hash mismatch: $name" }
        }
        foreach ($name in $Expected.Keys) {
            Replace-File (Join-Path $backup ("package_" + $name)) (Join-Path $target $name) $backup
            if ((Hash-File (Join-Path $target $name)) -ne $Expected[$name]) { throw "Installed hash mismatch: $name" }
        }
    }
    catch {
        foreach ($name in $Expected.Keys) {
            if ((Hash-File (Join-Path $target $name)) -ne $before[$name]) {
                $restore = Join-Path $backup ("restore_" + $name)
                [IO.File]::Copy((Join-Path $backup $name), $restore, $true)
                Replace-File $restore (Join-Path $target $name) $backup
                if ((Hash-File (Join-Path $target $name)) -ne $before[$name]) { throw 'Apply failed and automatic restore failed.' }
            }
        }
        throw
    }
    Write-Output "APPLIED BACKUP=$backup"
    return
}

if (-not $BackupPath) { throw 'BackupPath is required for Rollback.' }
$backup = [IO.Path]::GetFullPath($BackupPath)
$manifestPath = Join-Path $backup 'manifest.json'
Assert-RegularFile $manifestPath
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
if ($manifest.version -ne 1 -or $manifest.target -ne $target) { throw 'Backup manifest mismatch.' }
foreach ($name in $Expected.Keys) {
    if ($manifest.package.$name -ne $Expected[$name]) { throw 'Backup package mismatch.' }
    Assert-RegularFile (Join-Path $backup $name)
    Assert-RegularFile (Join-Path $target $name)
    if ((Hash-File (Join-Path $backup $name)) -ne $manifest.original.$name) { throw 'Backup bytes mismatch.' }
    $currentHash = Hash-File (Join-Path $target $name)
    if ($currentHash -ne $Expected[$name] -and $currentHash -ne $manifest.original.$name) {
        throw 'Installed bytes changed outside Apply/Rollback.'
    }
}
foreach ($name in $Expected.Keys) {
    if ((Hash-File (Join-Path $target $name)) -ne $manifest.original.$name) {
        $restore = Join-Path $backup ("restore_" + $name)
        [IO.File]::Copy((Join-Path $backup $name), $restore, $true)
        Replace-File $restore (Join-Path $target $name) $backup
        if ((Hash-File (Join-Path $target $name)) -ne $manifest.original.$name) { throw 'Rollback verification failed.' }
    }
}
Write-Output 'ROLLED_BACK'
