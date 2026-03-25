param(
  [Parameter(Mandatory=$true)]
  [string]$LogPath
)

if (!(Test-Path $LogPath)) {
  Write-Host "Log file not found: $LogPath" -ForegroundColor Red
  exit 1
}

$buyCount       = (Select-String -Path $LogPath -Pattern "BUY:" | Measure-Object).Count
$tpSellCount    = (Select-String -Path $LogPath -Pattern "SELL:.*reason=take_profit" | Measure-Object).Count
$slSellCount    = (Select-String -Path $LogPath -Pattern "SELL:.*reason=stop_loss" | Measure-Object).Count

$skipVolume     = (Select-String -Path $LogPath -Pattern "buy skip \| low volume" | Measure-Object).Count
$skipSpread     = (Select-String -Path $LogPath -Pattern "buy skip \| spread" | Measure-Object).Count
$skipTrend      = (Select-String -Path $LogPath -Pattern "buy skip \| trend filter" | Measure-Object).Count

$summary = [PSCustomObject]@{
  DateUTC            = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
  LogFile            = $LogPath
  BUY                = $buyCount
  SELL_TAKE_PROFIT   = $tpSellCount
  SELL_STOP_LOSS     = $slSellCount
  SKIP_LOW_VOLUME    = $skipVolume
  SKIP_SPREAD        = $skipSpread
  SKIP_TREND         = $skipTrend
}

$summary | Format-List

$csvPath = ".\\logs\\daily_summary.csv"
if (!(Test-Path (Split-Path $csvPath -Parent))) {
  New-Item -ItemType Directory -Force -Path (Split-Path $csvPath -Parent) | Out-Null
}

if (Test-Path $csvPath) {
  $summary | Export-Csv -Path $csvPath -NoTypeInformation -Append
} else {
  $summary | Export-Csv -Path $csvPath -NoTypeInformation
}

Write-Host "Saved summary to $csvPath"
