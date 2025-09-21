$BASE = "http://127.0.0.1:8000"
$H = @{ 'x-api-key'='supersecret123' }

# Reset & ensemble
Invoke-RestMethod -Method Post -Uri "$BASE/admin/preset/test" -Headers $H | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" -Body (@{ strategy_mode = "ensemble" } | ConvertTo-Json) | Out-Null
Invoke-RestMethod -Method Post -Uri "$BASE/admin/clear-latches" -Headers $H | Out-Null

# --- Per-day block: allow all days except today (Python weekday 0=Mon..6=Sun)
$dow_dotnet = (Get-Date).ToUniversalTime().DayOfWeek.value__   # Sun=0..Sat=6
$dow_py = ($dow_dotnet + 6) % 7                                # Mon=0..Sun=6
$allow = (0..6 | Where-Object { $_ -ne $dow_py })
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" -Body (@{
  allowed_days_utc = 0..6
  symbol_days = @{ EURUSD = @($allow) }
} | ConvertTo-Json) | Out-Null
"Signal (should be day_blocked_*):"
Invoke-RestMethod -Uri "$BASE/signal?symbol=EURUSD&tf=M5" -Headers $H

# Clear per-symbol days
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" -Body (@{
  symbol_days = @{}
} | ConvertTo-Json) | Out-Null

# --- Symbol loss cap + latch, then surface 'symbol_latched'
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" -Body (@{
  symbol_caps = @{ EURUSD = @{ max_daily_loss_abs = 1; cap_latch = $true } }
} | ConvertTo-Json) | Out-Null

# Cause loss
Invoke-RestMethod -Method Post -Uri "$BASE/logtrade" -Headers $H -ContentType "application/json" -Body (@{
  ts=(Get-Date).ToUniversalTime().ToString("o")
  status="CLOSE"; symbol="EURUSD"; dir=1; pips=-2; profit=-2; net=-2; ticket="SYM_LATCH"
} | ConvertTo-Json) | Out-Null

# Keep latch ON but remove the cap so the reason shows 'symbol_latched'
Invoke-RestMethod -Method Post -Uri "$BASE/config" -Headers $H -ContentType "application/json" -Body (@{
  symbol_caps = @{ EURUSD = @{ cap_latch = $true } }
} | ConvertTo-Json) | Out-Null

"Signal (should be symbol_latched):"
Invoke-RestMethod -Uri "$BASE/signal?symbol=EURUSD&tf=M5" -Headers $H
