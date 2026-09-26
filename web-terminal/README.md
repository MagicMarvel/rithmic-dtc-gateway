# Rithmic Web Terminal

Member-facing Web terminal backed exclusively by the
[`rithmic-dtc-gateway`](https://github.com/MagicMarvel/rithmic-dtc-gateway).
The browser application never receives the server market-data credentials.

It provides K-line, footprint, Bid × Ask, delta/volume profiles, MarketDepth,
large-order and anchored VWAP indicators, trend-line and fixed-range volume
profile tools, Paper order entry, and responsive phone landscape layouts.

## Run

Set `DTC_GATEWAY_ADDR`, `DTC_GATEWAY_ADMIN_URL`, `DTC_ADMIN_TOKEN`, and member
authentication variables from `deploy/web.env.example`, then run:

```bash
cargo run --release
```

The default listener is `127.0.0.1:11200`. All market and history requests use
the DTC gateway. Personal Rithmic Paper-order settings are sent only to the
gateway's private admin endpoint.

## Container deployment

Build with `Dockerfile`. `deploy/compose.yaml` runs both repository images on a
private Docker network and publishes only Web on `127.0.0.1:11200`. No database
is required; state is stored in bind-mounted `data` directories.
