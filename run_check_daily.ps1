# Daily auction check (weekdays 09:26)
$ErrorActionPreference = 'Continue'
$logDir = Join-Path $PSScriptRoot 'logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
& (Join-Path $PSScriptRoot 'run_node.ps1') (Join-Path $PSScriptRoot 'screener.js') '--check' *> (Join-Path $logDir 'check.log')
