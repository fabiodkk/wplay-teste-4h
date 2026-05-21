$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$pidFile = Join-Path $root 'public_link.pids.json'

if (-not (Test-Path $pidFile)) {
  Write-Output 'Nenhum pid salvo em public_link.pids.json'
  exit 0
}

$data = Get-Content $pidFile -Raw | ConvertFrom-Json
foreach ($pid in @($data.app_pid, $data.tunnel_pid)) {
  if ($pid) {
    try {
      Stop-Process -Id $pid -Force -ErrorAction Stop
      Write-Output "Parado PID $pid"
    } catch {
      Write-Output "PID $pid ja nao estava ativo"
    }
  }
}
