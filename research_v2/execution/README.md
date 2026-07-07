# Execution

Future work: implement realistic execution simulation mode isolated from research labeling mode.

## MEXC Pole live config example

`mexc_pole_live_config.example.json` is a documentation-only safe-default example for the isolated MEXC Pole live runner. It keeps production execution disabled by default:

- `live_trading_enabled`: `false`
- `dry_run`: `true`
- `fixed_risk_usdt`: `1.0`
- `max_open_positions`: `1`
- `max_daily_loss_usdt`: `3.0`
- `max_notional_usdt`: `100.0`
- `symbols`: `MEXC_FUT:BTCUSDT`, `MEXC_FUT:ETHUSDT`, `MEXC_FUT:SOLUSDT`, `MEXC_FUT:SUIUSDT`, `MEXC_FUT:ENAUSDT`

`symbols` is the canonical live universe key. The legacy `allowed_symbols` key is accepted only as a backward-compatible fallback when `symbols` is absent.

Exact dry-run command:

```bash
python mexc_pole_live_trader.py --config mexc_pole_live_config.example.json
```

Required credential environment variable names documented by the example:

```bash
export MEXC_FUTURES_API_KEY="<your-mexc-api-key>"
export MEXC_FUTURES_API_SECRET="<your-mexc-api-secret>"
```

Alternatively, place credentials in a local `mexc_credentials.json` file at the repository root; this file takes priority over environment variables and must not be committed:

```json
{
  "api_key": "<your-mexc-api-key>",
  "api_secret": "<your-mexc-api-secret>"
}
```

The example remains dry-run only until the config is deliberately changed away from the documented safe defaults.
