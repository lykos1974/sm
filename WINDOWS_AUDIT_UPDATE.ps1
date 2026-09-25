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

$PackageSourceCommit = "42d4cf525d627a8e8e8bce24d68871e3b54a302a"

$ExpectedFiles = [ordered]@{
    "live_mexc_forward_trader.py" = "844b60cfed374fc665fc91aad9524403536e13fc2229ba44e57a5391a3f71f48"
    "pnf_mvp/app.py" = "e6e4686922e38a0c5588c3b44a22be93e751384232740b9fb435cc7cd5b7a6c9"
    "pnf_mvp/pnf_engine.py" = "b561484175655da7b2327f5fea24f930ff5eeced7fb0d2344dc5e258894d4e9e"
    "pnf_mvp/storage.py" = "7456e19757c557607f5985461f5b34baa628a2069560ded3ac72f276553d7f6e"
    "pnf_mvp/strategy_historical_backfill.py" = "14c58e6983b74c7c1fee9e7f6817438fb48a54964552f566b930ea70319963fd"
    "pnf_mvp/strategy_setup_observer.py" = "60ae21e82266b9c371261dbcb83634d301447224b8bf66ab82b6a04ccdab5289"
    "pnf_mvp/strategy_validation.py" = "089b847ac55feacb8537b48ab3df90d2eed7768ea51874ddf1d866c4c290f3d2"
    "pnf_mvp/strategy_trade_export.py" = "6b039c473c31453d4afeb185d9e7c15050387f2a2eca5d61fd7d6a4501499383"
    "pnf_mvp/strategy_evaluator.py" = "37bf22f9bf42fbd8546b8aaddfe1b91ae3780371550736d63c75d894b331c467"
    "pnf_mvp/validation_tick_provenance_preflight.py" = "d16c7ab4a755dd60b1a8dac60d30510fb1ccc1d4d5ec62e1b93e37d1589b0e7c"
    "pnf_mvp/data/tick_provenance/strategy_validation_tick_provenance.json" = "8ad27ceb2d89d2e9ab57b954240189982fdbaa5ceb02f99d474f540ea2dfa562"
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

function Assert-Package([string]$Root) {
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
}

function Write-CanonicalLfFile([string]$PathValue) {
    $utf8 = [System.Text.UTF8Encoding]::new($false, $true)
    $text = $utf8.GetString([IO.File]::ReadAllBytes($PathValue))
    [IO.File]::WriteAllBytes($PathValue, $utf8.GetBytes($text.Replace("`r`n", "`n")))
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
            if ((Get-Sha256 $destination) -ne [string]$file.before_sha256) {
                throw "Rollback byte verification failed: $($file.relative_path)"
            }
        }
        elseif (Test-Path -LiteralPath $destination) {
            Remove-Item -LiteralPath $destination -Force
        }
        if (-not [bool]$file.existed -and (Test-Path -LiteralPath $destination)) {
            throw "Rollback failed to remove new file: $($file.relative_path)"
        }
    }
}

function Assert-Backups([object]$Manifest, [string]$BackupRoot, [string]$Target) {
    if ((Resolve-FullPath ([string]$Manifest.target_root)) -ne $Target) {
        throw "Rollback target does not match the manifest target."
    }
    if (@($Manifest.files).Count -ne $ExpectedFiles.Count) {
        throw "Rollback manifest file count mismatch."
    }
    foreach ($entry in $ExpectedFiles.GetEnumerator()) {
        $files = @($Manifest.files | Where-Object { $_.relative_path -eq $entry.Key })
        if ($files.Count -ne 1 -or [string]$files[0].expected_sha256 -ne $entry.Value) {
            throw "Rollback manifest mismatch: $($entry.Key)"
        }
        $file = $files[0]
        $expectedBackup = Get-RelativeFile (Join-Path $BackupRoot "code") $entry.Key
        if ((Resolve-FullPath ([string]$file.backup_path)) -ne (Resolve-FullPath $expectedBackup)) {
            throw "Rollback backup path mismatch: $($entry.Key)"
        }
        if ([bool]$file.existed -and ((-not (Test-Path -LiteralPath $expectedBackup -PathType Leaf)) -or
                (Get-Sha256 $expectedBackup) -ne [string]$file.before_sha256)) {
            throw "Rollback backup hash mismatch: $($entry.Key)"
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
    Assert-Package $SourceRoot
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
    [void](Assert-SettingsOff (Join-Path $TargetRoot "pnf_mvp\settings.json"))
    Assert-Backups $manifest $BackupPath $TargetRoot
    $rollbackSettingsHash = Get-Sha256 (Join-Path $TargetRoot "pnf_mvp\settings.json")
    Restore-Code $manifest $TargetRoot
    if ($RestoreDatabases) {
        Restore-Databases $manifest
    }
    [void](Assert-SettingsOff (Join-Path $TargetRoot "pnf_mvp\settings.json"))
    if ((Get-Sha256 (Join-Path $TargetRoot "pnf_mvp\settings.json")) -ne $rollbackSettingsHash) {
        throw "Operator settings changed during Rollback."
    }
    Write-Host "ROLLED BACK: code restored from $BackupPath"
    if (-not $RestoreDatabases) {
        Write-Host "Database backups retained; add -RestoreDatabases only when loss of post-update data is acceptable."
    }
    exit 0
}

if ($SourceRoot -eq $TargetRoot) {
    throw "SourceRoot and TargetRoot must be different for a reversible update."
}
Assert-Package $SourceRoot
$targetSettingsPath = Join-Path $TargetRoot "pnf_mvp\settings.json"
$targetSettings = Assert-SettingsOff $targetSettingsPath
$settingsHash = Get-Sha256 $targetSettingsPath

$backupRoot = Join-Path $TargetRoot "_audit_update_backups"
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss_fffffff") + "_" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
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
    settings_sha256 = $settingsHash
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
        if ($entry.Key -eq "pnf_mvp/data/tick_provenance/strategy_validation_tick_provenance.json") {
            Write-CanonicalLfFile $temporary
            if ((Get-Sha256 $temporary) -ne $entry.Value) {
                throw "Snapshot raw SHA-256 verification failed: $($entry.Key)"
            }
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
    if ((Get-Sha256 $targetSettingsPath) -ne $settingsHash) {
        throw "Operator settings changed during Apply."
    }
}
catch {
    $savedError = $_
    $loadedManifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    Assert-Backups $loadedManifest $BackupPath $TargetRoot
    Restore-Code $loadedManifest $TargetRoot
    throw "Update failed and code was restored automatically. Backup: $BackupPath. Error: $savedError"
}

$latestPath = Join-Path $backupRoot "LATEST.txt"
Set-Content -LiteralPath $latestPath -Value $BackupPath -Encoding ASCII
Write-Host "UPDATED AND VERIFIED: $TargetRoot"
Write-Host "BACKUP: $BackupPath"
Write-Host "No service was started; validation and alerts remain OFF."
