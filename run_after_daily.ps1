# Daily post-auction confirmation (weekdays 09:35): re-run only auction-qualified stocks
$ErrorActionPreference = 'Continue'
$logDir = Join-Path $PSScriptRoot 'logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
& (Join-Path $PSScriptRoot 'run_node.ps1') (Join-Path $PSScriptRoot 'screener.js') '--after' *> (Join-Path $logDir 'after.log')
