# Daily auction gap-up summary (weekdays; run from 09:25:10)
$ErrorActionPreference = 'Continue'
$logDir = Join-Path $PSScriptRoot 'logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
& (Join-Path $PSScriptRoot 'run_node.ps1') (Join-Path $PSScriptRoot 'auction_gapup.js') *> (Join-Path $logDir 'gapup.log')
