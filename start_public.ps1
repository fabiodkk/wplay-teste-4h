$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$outFile = Join-Path $root 'cloudflared.out.log'
$errFile = Join-Path $root 'cloudflared.err.log'
$pidFile = Join-Path $root 'public_link.pids.json'

if (Test-Path $outFile) { Remove-Item $outFile -Force }
if (Test-Path $errFile) { Remove-Item $errFile -Force }

$appProc = Start-Process -FilePath python -ArgumentList 'app.py' -WorkingDirectory $root -WindowStyle Hidden -PassThru
Start-Sleep -Seconds 3

$cfProc = Start-Process -FilePath cloudflared -ArgumentList 'tunnel','--url','http://127.0.0.1:5000','--no-autoupdate' -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput $outFile -RedirectStandardError $errFile -PassThru

$maxWait = 30
$url = $null
for ($i=0; $i -lt $maxWait; $i++) {
    Start-Sleep -Seconds 1
    if (Test-Path $errFile) {
        $line = Get-Content $errFile | Select-String -Pattern 'https://.*trycloudflare.com' | Select-Object -Last 1
        if ($line) {
            $match = [regex]::Match($line.Line, 'https://[a-zA-Z0-9\-\.]+\.trycloudflare\.com')
            if ($match.Success) {
                $url = $match.Value
                break
            }
        }
    }
}

@{
  app_pid = $appProc.Id
  tunnel_pid = $cfProc.Id
  public_url = $url
  created_at = (Get-Date).ToString('s')
} | ConvertTo-Json | Set-Content -Path $pidFile -Encoding utf8

if (-not $url) {
    Write-Output 'Public URL nao encontrada. Veja cloudflared.err.log'
    exit 1
}

Write-Output "Public URL: $url"
Write-Output "App PID: $($appProc.Id)"
Write-Output "Tunnel PID: $($cfProc.Id)"
