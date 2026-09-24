[CmdletBinding()]
param(
    [ValidateSet("Validate", "Apply", "Rollback")]
    [string]$Mode = "Validate",
    [string]$SourceRoot,
    [string]$TargetRoot,
    [string]$BackupPath,
    [switch]$ConfirmServicesStopped,
    [switch]$RestoreDatabases
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

$PackageSourceCommit = "ea257e43ca168b5e7e70f900a896c4434fb3fbc9"

$ExpectedFiles = [ordered]@{
    "pnf_mvp/app.py" = "58158acef33892de298c97560b6314dfd8d934682770cf70047c87fec3092eb7"
    "pnf_mvp/pnf_engine.py" = "b561484175655da7b2327f5fea24f930ff5eeced7fb0d2344dc5e258894d4e9e"
    "pnf_mvp/storage.py" = "7456e19757c557607f5985461f5b34baa628a2069560ded3ac72f276553d7f6e"
    "pnf_mvp/strategy_historical_backfill.py" = "14c58e6983b74c7c1fee9e7f6817438fb48a54964552f566b930ea70319963fd"
    "pnf_mvp/strategy_setup_observer.py" = "60ae21e82266b9c371261dbcb83634d301447224b8bf66ab82b6a04ccdab5289"
    "pnf_mvp/strategy_validation.py" = "089b847ac55feacb8537b48ab3df90d2eed7768ea51874ddf1d866c4c290f3d2"
    "pnf_mvp/strategy_trade_export.py" = "6b039c473c31453d4afeb185d9e7c15050387f2a2eca5d61fd7d6a4501499383"
    "pnf_mvp/strategy_evaluator.py" = "37bf22f9bf42fbd8546b8aaddfe1b91ae3780371550736d63c75d894b331c467"
}

function Resolve-FullPath([string]$PathValue) {
    return [IO.Path]::GetFullPath($PathValue)
}

function Get-RelativeFile([string]$Root, [string]$RelativePath) {
    return Join-Path $Root ($RelativePath.Replace("/", [IO.Path]::DirectorySeparatorChar))
}

function Get-Sha256([string]$PathValue) {
    return (Get-FileHash -LiteralPath $PathValue -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-CanonicalTextSha256([string]$PathValue) {
    $strictUtf8 = [System.Text.UTF8Encoding]::new($false, $true)
    try {
        $text = $strictUtf8.GetString([IO.File]::ReadAllBytes($PathValue))
    }
    catch {
        throw "Canonical package file is not valid UTF-8: $PathValue"
    }
    $canonicalText = $text.Replace("`r`n", "`n")
    $canonicalBytes = $strictUtf8.GetBytes($canonicalText)
    $sha256 = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha256.ComputeHash($canonicalBytes))).Replace("-", "").ToLowerInvariant()
    }
    finally {
        $sha256.Dispose()
    }
}

function Get-PythonCommand {
    $python = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($python) {
        return @{ Executable = $python.Source; Prefix = @() }
    }
    $launcher = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($launcher) {
        return @{ Executable = $launcher.Source; Prefix = @("-3") }
    }
    throw "Existing Python runtime not found; no package was changed."
}

function Invoke-Python([hashtable]$Python, [string[]]$Arguments) {
    $allArguments = @($Python.Prefix) + $Arguments
    & $Python.Executable @allArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Python verification failed with exit code $LASTEXITCODE."
    }
}

function Assert-SettingsOff([string]$SettingsPath) {
    if (-not (Test-Path -LiteralPath $SettingsPath -PathType Leaf)) {
        throw "Missing settings file: $SettingsPath"
    }
    $settings = Get-Content -LiteralPath $SettingsPath -Raw | ConvertFrom-Json
    if ($settings.strategy_validation_enabled -ne $false) {
        throw "strategy_validation_enabled must remain false: $SettingsPath"
    }
    if ($settings.operational_alerts_enabled -ne $false) {
        throw "operational_alerts_enabled must remain false: $SettingsPath"
    }
    return $settings
}

function Assert-Package([string]$Root, [hashtable]$Python) {
    foreach ($entry in $ExpectedFiles.GetEnumerator()) {
        $path = Get-RelativeFile $Root $entry.Key
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Required package file missing: $path"
        }
        $actual = Get-CanonicalTextSha256 $path
        if ($actual -ne $entry.Value) {
            throw "Package hash mismatch for $($entry.Key): $actual"
        }
    }
    $settingsPath = Join-Path $Root "pnf_mvp\settings.json"
    [void](Assert-SettingsOff $settingsPath)
    $env:AUDIT_PNF_ROOT = Join-Path $Root "pnf_mvp"
    try {
        Invoke-Python $Python @("-m", "compileall", "-q", $env:AUDIT_PNF_ROOT)
        Invoke-Python $Python @(
            "-c",
            "import os,sys;sys.path.insert(0,os.environ['AUDIT_PNF_ROOT']);import app,storage,pnf_engine,structure_engine,strategy_engine,strategy_validation,strategy_setup_observer,strategy_historical_backfill"
        )
    }
    finally {
        Remove-Item Env:AUDIT_PNF_ROOT -ErrorAction SilentlyContinue
    }
}

function Resolve-DatabasePath([string]$Value, [string]$PnfRoot) {
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $null
    }
    if ([IO.Path]::IsPathRooted($Value)) {
        return Resolve-FullPath $Value
    }
    return Resolve-FullPath (Join-Path $PnfRoot $Value)
}

function Get-DatabasePaths([object]$Settings, [string]$PnfRoot) {
    $paths = New-Object System.Collections.Generic.List[string]
    $values = @()
    if ($Settings.PSObject.Properties.Name -contains "database_path") {
        $values += [string]$Settings.database_path
    }
    if ($Settings.PSObject.Properties.Name -contains "strategy_validation_db_path") {
        $values += [string]$Settings.strategy_validation_db_path
    }
    else {
        $values += "strategy_validation.db"
    }
    if (($Settings.PSObject.Properties.Name -contains "ideal_entry_observer") -and $Settings.ideal_entry_observer) {
        if ($Settings.ideal_entry_observer.PSObject.Properties.Name -contains "database_path") {
            $values += [string]$Settings.ideal_entry_observer.database_path
        }
    }
    foreach ($value in $values) {
        $resolved = Resolve-DatabasePath $value $PnfRoot
        if ($resolved -and -not $paths.Contains($resolved)) {
            $paths.Add($resolved)
        }
    }
    return @($paths)
}

function Restore-Code([object]$Manifest, [string]$Target) {
    foreach ($file in $Manifest.files) {
        $destination = Get-RelativeFile $Target ([string]$file.relative_path)
        if ([bool]$file.existed) {
            $parent = Split-Path -Parent $destination
            New-Item -ItemType Directory -Path $parent -Force | Out-Null
            Copy-Item -LiteralPath ([string]$file.backup_path) -Destination $destination -Force
        }
        elseif (Test-Path -LiteralPath $destination) {
            Remove-Item -LiteralPath $destination -Force
        }
    }
}

function Restore-Databases([object]$Manifest) {
    foreach ($database in $Manifest.databases) {
        $destination = [string]$database.original_path
        $parent = Split-Path -Parent $destination
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
        Copy-Item -LiteralPath ([string]$database.backup_path) -Destination $destination -Force
        foreach ($suffix in @("-wal", "-shm")) {
            $backupSidecar = ([string]$database.backup_path) + $suffix
            $destinationSidecar = $destination + $suffix
            if (Test-Path -LiteralPath $backupSidecar) {
                Copy-Item -LiteralPath $backupSidecar -Destination $destinationSidecar -Force
            }
            elseif (Test-Path -LiteralPath $destinationSidecar) {
                Remove-Item -LiteralPath $destinationSidecar -Force
            }
        }
    }
}

if ([string]::IsNullOrWhiteSpace($SourceRoot)) {
    $SourceRoot = $PSScriptRoot
}
if ([string]::IsNullOrWhiteSpace($SourceRoot)) {
    $scriptPath = $MyInvocation.MyCommand.Path
    if ([string]::IsNullOrWhiteSpace($scriptPath)) {
        throw "SourceRoot could not be derived; pass -SourceRoot explicitly."
    }
    $SourceRoot = Split-Path -Parent $scriptPath
}
$SourceRoot = Resolve-FullPath $SourceRoot

if ($Mode -eq "Validate") {
    $pythonCommand = Get-PythonCommand
    Assert-Package $SourceRoot $pythonCommand
    Write-Host "VALIDATED: consolidated audit package; validation and alerts remain OFF."
    exit 0
}

if ([string]::IsNullOrWhiteSpace($TargetRoot)) {
    throw "TargetRoot is required for Apply or Rollback."
}
$TargetRoot = Resolve-FullPath $TargetRoot

if (-not $ConfirmServicesStopped) {
    throw "Stop scanner, collector, and trader processes, then pass -ConfirmServicesStopped."
}

if ($Mode -eq "Rollback") {
    if ([string]::IsNullOrWhiteSpace($BackupPath)) {
        throw "BackupPath is required for Rollback."
    }
    $BackupPath = Resolve-FullPath $BackupPath
    $manifestPath = Join-Path $BackupPath "manifest.json"
    if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
        throw "Rollback manifest not found: $manifestPath"
    }
    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    if ((Resolve-FullPath ([string]$manifest.target_root)) -ne $TargetRoot) {
        throw "Rollback target does not match the manifest target."
    }
    Restore-Code $manifest $TargetRoot
    if ($RestoreDatabases) {
        Restore-Databases $manifest
    }
    [void](Assert-SettingsOff (Join-Path $TargetRoot "pnf_mvp\settings.json"))
    Write-Host "ROLLED BACK: code restored from $BackupPath"
    if (-not $RestoreDatabases) {
        Write-Host "Database backups retained; add -RestoreDatabases only when loss of post-update data is acceptable."
    }
    exit 0
}

if ($SourceRoot -eq $TargetRoot) {
    throw "SourceRoot and TargetRoot must be different for a reversible update."
}
$pythonCommand = Get-PythonCommand
Assert-Package $SourceRoot $pythonCommand
$targetSettingsPath = Join-Path $TargetRoot "pnf_mvp\settings.json"
$targetSettings = Assert-SettingsOff $targetSettingsPath

$backupRoot = Join-Path $TargetRoot "_audit_update_backups"
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupPath = Join-Path $backupRoot $stamp
$codeBackup = Join-Path $BackupPath "code"
$databaseBackup = Join-Path $BackupPath "databases"
New-Item -ItemType Directory -Path $codeBackup -Force | Out-Null
New-Item -ItemType Directory -Path $databaseBackup -Force | Out-Null

$fileManifest = @()
foreach ($entry in $ExpectedFiles.GetEnumerator()) {
    $source = Get-RelativeFile $SourceRoot $entry.Key
    $target = Get-RelativeFile $TargetRoot $entry.Key
    $backup = Get-RelativeFile $codeBackup $entry.Key
    $existed = Test-Path -LiteralPath $target -PathType Leaf
    $beforeHash = $null
    if ($existed) {
        $beforeHash = Get-Sha256 $target
        New-Item -ItemType Directory -Path (Split-Path -Parent $backup) -Force | Out-Null
        Copy-Item -LiteralPath $target -Destination $backup -Force
    }
    $fileManifest += [ordered]@{
        relative_path = $entry.Key
        existed = [bool]$existed
        backup_path = $backup
        before_sha256 = $beforeHash
        expected_sha256 = $entry.Value
    }
}

$databaseManifest = @()
$databaseIndex = 0
foreach ($database in (Get-DatabasePaths $targetSettings (Join-Path $TargetRoot "pnf_mvp"))) {
    if (Test-Path -LiteralPath $database -PathType Leaf) {
        $backup = Join-Path $databaseBackup ("db_{0}.sqlite3" -f $databaseIndex)
        Copy-Item -LiteralPath $database -Destination $backup -Force
        foreach ($suffix in @("-wal", "-shm")) {
            if (Test-Path -LiteralPath ($database + $suffix) -PathType Leaf) {
                Copy-Item -LiteralPath ($database + $suffix) -Destination ($backup + $suffix) -Force
            }
        }
        $databaseManifest += [ordered]@{
            original_path = $database
            backup_path = $backup
            sha256 = Get-Sha256 $database
        }
        $databaseIndex += 1
    }
}

$manifest = [ordered]@{
    package_source_commit = $PackageSourceCommit
    created_utc = [DateTime]::UtcNow.ToString("o")
    source_root = $SourceRoot
    target_root = $TargetRoot
    files = $fileManifest
    databases = $databaseManifest
}
$manifestPath = Join-Path $BackupPath "manifest.json"
$manifest | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $manifestPath -Encoding UTF8

try {
    foreach ($entry in $ExpectedFiles.GetEnumerator()) {
        $source = Get-RelativeFile $SourceRoot $entry.Key
        $target = Get-RelativeFile $TargetRoot $entry.Key
        $parent = Split-Path -Parent $target
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
        $temporary = $target + ".audit-update.tmp"
        Copy-Item -LiteralPath $source -Destination $temporary -Force
        if ((Get-CanonicalTextSha256 $temporary) -ne $entry.Value) {
            throw "Temporary copy verification failed: $($entry.Key)"
        }
        Move-Item -LiteralPath $temporary -Destination $target -Force
    }
    foreach ($entry in $ExpectedFiles.GetEnumerator()) {
        $target = Get-RelativeFile $TargetRoot $entry.Key
        if ((Get-CanonicalTextSha256 $target) -ne $entry.Value) {
            throw "Installed file verification failed: $($entry.Key)"
        }
    }
    [void](Assert-SettingsOff $targetSettingsPath)
    $targetPnfRoot = Join-Path $TargetRoot "pnf_mvp"
    $env:AUDIT_PNF_ROOT = $targetPnfRoot
    try {
        Invoke-Python $pythonCommand @("-m", "compileall", "-q", $targetPnfRoot)
        Invoke-Python $pythonCommand @(
            "-c",
            "import os,sys;sys.path.insert(0,os.environ['AUDIT_PNF_ROOT']);import app,storage,pnf_engine,structure_engine,strategy_engine,strategy_validation,strategy_setup_observer,strategy_historical_backfill"
        )
    }
    finally {
        Remove-Item Env:AUDIT_PNF_ROOT -ErrorAction SilentlyContinue
    }
}
catch {
    $savedError = $_
    $loadedManifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    Restore-Code $loadedManifest $TargetRoot
    throw "Update failed and code was restored automatically. Backup: $BackupPath. Error: $savedError"
}

$latestPath = Join-Path $backupRoot "LATEST.txt"
Set-Content -LiteralPath $latestPath -Value $BackupPath -Encoding ASCII
Write-Host "UPDATED AND VERIFIED: $TargetRoot"
Write-Host "BACKUP: $BackupPath"
Write-Host "No service was started; validation and alerts remain OFF."
