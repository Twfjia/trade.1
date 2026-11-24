# Daily pre-market candidate generation (weekdays 08:45)
$ErrorActionPreference = 'Continue'
$logDir = Join-Path $PSScriptRoot 'logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
& (Join-Path $PSScriptRoot 'run_node.ps1') (Join-Path $PSScriptRoot 'screener.js') '--premarket' *> (Join-Path $logDir 'premarket.log')
