$ErrorActionPreference = "Stop"

function Assert-Reason($actual, $pattern, $label) {
  if ($actual -notmatch $pattern) {
    throw "FAIL $($label): expected '$pattern' but got '$actual'"
  }
  Write-Host "PASS $($label) -> $actual"
}

$BASE = "http://127.0.0.1:8000"
$H = @{ 'x-api-key'='supersecret123' }

# --- Section 1: Global hour gate (deterministic) ---
Invoke-RestMethod -Method Post -Uri "$BASE/admin/preset/test" -Headers $H | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" `
  -Body (@{ strategy_mode = "ensemble" } | ConvertTo-Json) | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/admin/clear-latches" -Headers $H | Out-Null

Write-Host "`n== Global hour gate =="
$nowHr  = (Get-Date).ToUniversalTime().Hour
$blocked = (0..23 | Where-Object { $_ -ne $nowHr })[0]
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" `
  -Body (@{ allowed_hours_utc = @($blocked) } | ConvertTo-Json) | Out-Null
$r = Invoke-RestMethod -Uri "$BASE/signal?symbol=EURUSD&tf=M5" -Headers $H
Assert-Reason $r.reason '^hour_blocked_\d{2}Z$' 'Global hour gate'

# --- Section 2: Per-symbol hour override wins over global ---
Invoke-RestMethod -Method Post -Uri "$BASE/admin/preset/test" -Headers $H | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" `
  -Body (@{ strategy_mode = "ensemble" } | ConvertTo-Json) | Out-Null
Write-Host "`n== Per-symbol hour override (wins over global) =="
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" `
  -Body (@{ allowed_hours_utc = 0..23; symbol_hours = @{ EURUSD = @($blocked) } } | ConvertTo-Json) | Out-Null
$r = Invoke-RestMethod -Uri "$BASE/signal?symbol=EURUSD&tf=M5" -Headers $H
Assert-Reason $r.reason '^hour_blocked_\d{2}Z$' 'Per-symbol hour override'
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" `
  -Body (@{ symbol_hours = @{} } | ConvertTo-Json) | Out-Null

# --- Section 3: Cool-off after loss (deterministic) ---
Invoke-RestMethod -Method Post -Uri "$BASE/admin/preset/test" -Headers $H | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" `
  -Body (@{ strategy_mode = "ensemble" } | ConvertTo-Json) | Out-Null
Write-Host "`n== Cool-off after loss =="
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" `
  -Body (@{ symbol_cool_off = @{ EURUSD = @{ after_loss_min = 1; after_win_min = 0 } } } | ConvertTo-Json) | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/logtrade" -Headers $H -ContentType "application/json" `
  -Body (@{ ts=(Get-Date).ToUniversalTime().ToString("o"); status="CLOSE"; symbol="EURUSD"; dir=1; pips=-1; profit=-1; net=-1; ticket="CO_TEST" } | ConvertTo-Json) | Out-Null
$r = Invoke-RestMethod -Uri "$BASE/signal?symbol=EURUSD&tf=M5" -Headers $H
Assert-Reason $r.reason '^cooloff_active$' 'Cool-off after loss'

# --- Section 4: No-hedge per-symbol (force opposite with toy) ---
Invoke-RestMethod -Method Post -Uri "$BASE/admin/preset/test" -Headers $H | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" `
  -Body (@{ strategy_mode = "toy"; no_hedge = $false; symbol_caps = @{ EURUSD = @{ no_hedge = $true } } } | ConvertTo-Json) | Out-Null
Write-Host "`n== No-hedge per-symbol (overrides global) =="
Invoke-RestMethod -Method Post -Uri "$BASE/logtrade" -Headers $H -ContentType "application/json" `
  -Body (@{ ts=(Get-Date).ToUniversalTime().ToString("o"); status="OPEN"; symbol="EURUSD"; dir=1; entry=1.1000; lots=0.1; ticket="NH_TEST" } | ConvertTo-Json) | Out-Null
$r = Invoke-RestMethod -Uri "$BASE/signal?symbol=EURUSD&tf=M5" -Headers $H
Assert-Reason $r.reason '^no_hedge$' 'No-hedge per-symbol'

# --- Cleanup ---
Write-Host "`n== Cleanup back to test preset =="
Invoke-RestMethod -Method Post -Uri "$BASE/admin/preset/test" -Headers $H | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/admin/clear-latches" -Headers $H | Out-Null
Write-Host "Done."
