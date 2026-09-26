# ODT Rithmic Gateway + Web Terminal

This repository is a Rust workspace containing two independently deployable
applications:

- `dtc_server` is the Rithmic gateway. It owns the server market-data account,
  history cache, optional per-member Paper trading routes, and exposes only the
  standard DTC binary protocol to downstream applications.
- `rithmic-web-terminal` is the member-facing Web application. It depends on the
  gateway through DTC for live quotes, depth, history, accounts, and Paper order
  routing; it never receives the server's Rithmic credentials.

The production deployment uses `deploy/compose.yaml`. Only Web port 11200 is
bound to loopback for the TLS proxy; DTC and its private admin API stay on the
Compose network. No database is required. Runtime state is kept in bind-mounted
`data/gateway` and `data/web` directories.

```powershell
Copy-Item deploy/gateway.env.example deploy/gateway.env
Copy-Item deploy/web.env.example deploy/web.env
docker compose -f deploy/compose.yaml up -d
```

Build definitions are in `docker/gateway.Dockerfile` and
`docker/web.Dockerfile`.

A local Rust bridge from Rithmic to Sierra Chart. It uses
[`rithmic-rs`](https://crates.io/crates/rithmic-rs) for upstream connectivity and
implements Sierra Chart's official DTC binary protocol on `localhost`.

The bridge is usable today for:

- real-time Last Trade, Best Bid/Ask, and available session statistics;
- one-shot market-data and aggregated-depth snapshots;
- Time & Sales with at-bid/at-ask classification;
- a complete internal Rithmic Depth-by-Order (DBO) book, aggregated to DTC
  Market-by-Price/L2 for Sierra Chart;
- Chart DOM, Numbers Bars/Footprint, Volume Profile, and Delta;
- historical ticks, intraday bars, and daily OHLCV fallback bars;
- dynamic futures discovery, exchange/underlying enumeration, and a front-month futures catalog; and
- opt-in **Rithmic Paper Trading only** for standalone Market, Limit, Stop Market,
  and Stop Limit orders.

It does **not** patch or hook Sierra Chart, reimplement the Rithmic protocol,
advertise third-party MBO, or permit live-environment trading. Account credentials
are read only from `.env` or environment variables. The bridge generates one
random locally administered MAC address per process and never reads or transmits
the host's hardware MAC address.

## Status and design

The project was developed and accepted phase by phase:

| Phase | Result |
| --- | --- |
| 0 | Credentialed probe validated Last Trade, BBO, order book, and DBO on Paper Trading. |
| 1–2 | Local DTC binary session, security definition, Trade, and BBO. |
| 3–4 | Complete DBO book, aggregated L2, Sierra aliases, Time & Sales/Delta classification. |
| 5–6 | Rithmic reconnect supervision plus DTC heartbeat and stale-session handling. |
| 7 | Historical ticks, intraday bars, streaming chunks, and daily fallback. |
| 8 | Opt-in Paper-only trading, orders, positions, balances, and PnL. |
| 9–10 | Dynamic multi-symbol futures resolution and cumulative Sierra catalog publishing. |

Each DTC depth subscription owns an independent, tick-size-aware DBO book. The
initial snapshot is followed by incremental level updates; a desynchronized or
crossed book is withheld and rebuilt before another coherent snapshot is sent.
Rithmic market, history, order, and PnL plants are supervised independently.

## Requirements

- Windows with Sierra Chart (validated with 64-bit build 2945)
- a Rust toolchain supporting Edition 2024
- a Rithmic Paper Trading account with the required market-data permissions
- permission to connect to the Rithmic WebSocket endpoint supplied by the broker

## Quick start

1. Copy the example configuration:

   ```powershell
   Copy-Item .env.example .env
   ```

2. Fill the Rithmic Demo credentials and endpoints in `.env`. Never commit this
   file; it is excluded by `.gitignore`.

3. Set `RITHMIC_PROBE_SYMBOL` and `RITHMIC_PROBE_EXCHANGE` to a currently trading
   contract for which the account has real-time permission.

4. Validate the upstream account before starting Sierra integration:

   ```powershell
   cargo run --bin rithmic_probe
   ```

5. Only after all four probe checks pass, start the bridge:

   ```powershell
   cargo run --bin dtc_server
   ```

The default listener is `127.0.0.1:11099`; override it with
`DTC_LISTEN_ADDR`. Do not expose this unauthenticated development server to a
public or untrusted network.

## Probe acceptance criteria

The process exits successfully only after it has observed all of:

- Last Trade
- Best Bid/Ask
- Order Book (aggregated depth)
- Depth By Order

An accepted subscription alone does not pass validation. The probe must receive
actual messages in every category before `RITHMIC_PROBE_TIMEOUT_SECS` expires.
Credentials are loaded by `rithmic-rs` from environment-specific variables and
are never logged by this program.

## Local verification

```powershell
cargo fmt --check
cargo test
cargo build
```

## Run the DTC server

```powershell
cargo run --bin dtc_server
```

It logs into Rithmic first and then listens on `127.0.0.1:11099` by default.
Override this with `DTC_LISTEN_ADDR`. The server supports official fixed-length
Binary Encoding negotiation, logon, heartbeat, logoff, dynamic futures security definitions,
market-data subscribe/unsubscribe/snapshot, Last Trade, Best Bid/Ask, aggregated L2 market
depth, historical ticks, and historical intraday bars. Third-party MBO is never
advertised to Sierra Chart: the complete Rithmic Depth-by-Order book exists only
inside the bridge and is aggregated to Market-by-Price before it reaches DTC.

### DTC account router

The DTC process can use separate Rithmic credentials for market data/history and
Paper order/PnL. Open http://127.0.0.1:11101/, enter the admin token printed at
startup (or set DTC_ADMIN_TOKEN), edit either account, and choose **保存并切换**.
The replacement connections are established before the active route changes;
on success, existing Sierra sessions are closed so Sierra reconnects using the
new route. A failed replacement leaves the current route running.

The account file defaults to data/dtc/accounts.json, is excluded from Git, and
is mode 0600 on Unix. Password fields are write-only in the API: leave them
blank to keep the stored value. The management listener defaults to loopback and
must not be exposed publicly. Trading remains restricted to the Rithmic
Demo/Paper environment.

## Connect Sierra Chart

Create or edit a DTC Service connection in Sierra Chart with these values:

| Setting | Value |
| --- | --- |
| Server | `127.0.0.1` |
| Port | `11099` (or the port in `DTC_LISTEN_ADDR`) |
| Encoding | Binary |
| TLS | Disabled for this localhost connection |

Connect the data feed, open **Find Symbol**, and select a published contract or
enter a contract manually using Sierra's `SYMBOL-EXCHANGE` form, such as
`ESU6-CME`. Rithmic API requests use the corresponding `SYMBOL.EXCHANGE` pair
internally.

Sierra controls the requested L2 depth. Set **Number of Depth Levels to
Subscribe** on the exact symbol in **Global Settings > Symbol Settings**, enable
**Use Custom Symbol Settings Values**, apply the change, and reconnect the data
feed. A value of `1400` makes Sierra request up to 1400 bid and 1400 ask levels;
the bridge sends only levels that actually exist in the current Rithmic book.

The fallback instrument is read from `RITHMIC_DTC_SYMBOL` and
`RITHMIC_DTC_EXCHANGE`, falling back to the corresponding probe variables. Other
futures are resolved against Rithmic reference data when requested. Sierra 2945
was observed to send exact type-506 requests for existing or manually entered
symbols, but not exchange-list/search requests when opening Find Symbol. Therefore
the bridge proactively publishes up to 32 current front-month futures definitions
at logon and also registers manually resolved contracts on demand. Symbols can
appear in Sierra's `Other` category because core DTC security definitions do not
carry Sierra-specific category and rollover rules; this does not prevent charts,
market depth, or historical requests from using them.

## Rithmic Flow web terminal

The repository also includes a browser terminal using TradingView Lightweight
Charts 5.2.1. Real-time market data and history always come from the server's
Rithmic connection. A separate, optional user connection handles Paper orders,
accounts, positions, and P/L. The terminal supports ES, NQ, and GC tabs,
minute-bar history, live candles, best bid/ask, aggregated depth, Paper order
entry, positions, working orders, and cancel requests. Start it with:

```powershell
$env:DTC_GATEWAY_ADDR = "127.0.0.1:11099"
cargo run -p rithmic-web-terminal
```

The default URL is `http://127.0.0.1:11200/`; override it with
`TERMINAL_HTTP_LISTEN_ADDR`. Every terminal API, including the live WebSocket,
history, and order routes, requires a valid member session. Configure one member
with `TERMINAL_MEMBER_USER` and `TERMINAL_MEMBER_PASSWORD`, or multiple members
with `TERMINAL_MEMBERS_JSON`. Each JSON entry accepts `username`, `password`,
optional `active`, and optional Unix `expiresAt`. If no members are configured,
the server fails closed and the terminal shows a configuration message instead
of returning market data. Sessions use an HttpOnly, SameSite=Strict cookie and
default to 12 hours (`TERMINAL_MEMBER_SESSION_SECS`); set
`TERMINAL_COOKIE_SECURE=true` behind HTTPS. The built-in account source is
intended for a private deployment; use TLS and a proper membership service or
reverse proxy before exposing the terminal publicly. Set the Rithmic
environment variables in `.env` first. `RITHMIC_ENABLE_TRADING=true` enables
the existing Paper-only trading plant; otherwise the order ticket remains
read-only. The frontend asks for an explicit confirmation before every order.

Configuring a personal order account without restarting: the `下单账号` button in the
top bar opens a drawer with the environment (Paper Trading, Live, Test), the
system name (`Rithmic Paper Trading`, `Rithmic 01`, or a prop-firm system such
as `Apex` or `TopstepTrader`), the user, password, Paper Account/FCM/IB IDs, and
gateway URLs. `测试登录` logs in on a throwaway socket, reports which systems the
gateway offers, and does not disturb the server data feeds. `保存并连接下单`
establishes a replacement Paper order connection (when trading is enabled) and
atomically replaces only the order client. It never reloads, clears, or
reconnects market/history data. A failed replacement leaves the old order
account active. When `保存下单连接` is checked, the settings are written to
`data/rithmic-trading-connection.json` (override with
`RITHMIC_TRADING_CONNECTION_FILE`) so the terminal keeps using that order
account after a restart. `清除下单连接` returns the terminal to read-only mode
without affecting the chart. The drawer can also keep several account profiles in the
browser's local storage for one-click switching; passwords are only stored
there when explicitly confirmed. The endpoints are `GET/POST /api/connection`,
`POST /api/connection/test`, and `POST /api/connection/reset`, and the response
never includes the password or the server data credentials. The server data
connection, DTC server, and probes keep reading the environment only.

Chart toolbar features:

- **History download by days.** The `天数` box next to the period buttons
  selects how many calendar days (1-3650) of history to load; `下载` forces a
  refresh (`/api/history?...&days=N&refresh=1`). Browser charts use only the
  Rithmic History Plant and a separate local Rithmic cache; there is no public
  market-data fallback. The chosen day count is remembered per browser.
- **Footprint chart.** The `足迹图` chart type requests raw Rithmic trades and
  displays price-level Bid x Ask volume, per-bar delta, and the point of control.
  Live Rithmic trades update the open footprint bar in real time.
- **Large-order indicator.** Enable `大单成交` from `指标` and open its settings
  to choose the minimum individual Rithmic trade size, buy/sell colors, and
  labels. Historical time, Tick, and Range bars retain the largest individual
  Bid/Ask trade at every price level instead of treating aggregate bar volume as
  one order.
- **Anchored VWAP.** `VWAP` supports Session (configurable New York session
  start), week, month, year, first visible bar, and a custom date/time anchor,
  plus HLC3/OHLC4/Close source, color, and line width settings.
- **Drawing tools.** `趋势线` places a line with two chart clicks. `VP` selects a
  fixed time range and draws its volume-by-price profile and POC. Drawings are
  saved per symbol in browser storage; `清除` removes the current symbol's
  drawings.
- **Mobile landscape layout.** On phones and small tablets used sideways, the
  chart fills the first viewport, the dense toolbar becomes touch-scrollable,
  and order entry, depth, positions, orders, and statistics stack vertically
  below it. Safe-area insets and short landscape login/settings views are
  handled explicitly.
- **Time-axis zone switch.** The `纽约 / 北京` toggle at the bottom-right of the
  chart re-labels the time axis, crosshair, and feed timestamps in
  `America/New_York` or `Asia/Shanghai`. The choice is remembered per browser.
 - **Indicator menu (`指标`).** The only chart overlay is the MenthorQ intraday
   levels (`MQ`), toggled from the `基础叠加` group in the `指标` dropdown.
   Ticking `MQ` loads the current contract's levels when an API key is saved in
   `MQ 设置`, otherwise it opens that drawer. The choice is remembered per
   browser. The MenthorQ API key lives only in the browser's local storage and
   in the `MQ 设置` drawer you type into; it is never written to any file in
   this project, so every recipient has to paste their own key.

Run the credentialed Paper Trading end-to-end tests one at a time with:

```powershell
cargo test --test dtc_live paper_trading_es_flows_through_dtc_wire -- --ignored --nocapture
cargo test --test dtc_live paper_trading_es_dbo_flows_as_aggregated_dtc_l2 -- --ignored --nocapture
cargo test --test dtc_live paper_trading_dynamic_multi_symbol_market_data_flows_through_dtc_wire -- --ignored --nocapture
```

The first test passes only after receiving a real-time Trade V2 and Best Bid/Ask
V2 message over the DTC wire, including a classified at-bid/at-ask trade. The
second requests the same 1400 depth levels observed from Sierra Chart and passes
only after receiving an aggregated bid/ask snapshot followed by a live L2 update.
Running them separately avoids opening two concurrent Rithmic sessions for the
same trial account.

## Historical data

Historical data is enabled whenever the server starts successfully. Sierra can
request tick records or time bars through a dedicated DTC historical connection,
which remains open for multiple sequential requests as required by Sierra 2945.
The bridge applies the requested `MaxDays` relative to the end time, taking the
later of that boundary and Sierra's explicit start time. It imposes no additional
day cap. Tick history is fetched in bounded windows
(`RITHMIC_HISTORY_TICK_CHUNK_HOURS`, default: 6) and streamed to Sierra as each
window completes, keeping memory bounded and the DTC download active. Some Rithmic Paper systems
return no records for `DAILY_BAR`; in that case the bridge retries the same
fixed interval as server-generated 1440-minute OHLCV bars.
Tick records preserve at-bid/at-ask classification; bar records include bid and
ask volume for Numbers Bars, Volume Profile, and Delta calculations. A replay
can optionally be bounded with a positive `RITHMIC_HISTORY_REQUEST_TIMEOUT_SECS`;
the timeout is disabled when this setting is absent or zero.

Credentialed history checks are intentionally ignored by the ordinary test run:

```powershell
cargo test --test history_live -- --ignored --nocapture

# With dtc_server running on an otherwise unused test port:
cargo test --test dtc_live paper_trading_daily_history_flows_through_dtc_wire -- --ignored --nocapture
```

## Paper-only trading

Trading is off by default. To enable it, keep `RITHMIC_ENV=demo`, fill the three
Paper account identifiers, and set:

```dotenv
RITHMIC_ENABLE_TRADING=true
RITHMIC_DEMO_ACCOUNT_ID=...
RITHMIC_DEMO_FCM_ID=...
RITHMIC_DEMO_IB_ID=...
RITHMIC_MAX_ORDER_QUANTITY=1
RITHMIC_ALLOW_MARKET_ORDERS=false
RITHMIC_ORDER_SAFETY_CANCEL_SECS=30
```

The bridge supports standalone Market, Limit, Stop Market, and Stop Limit orders,
plus cancel/replace, DTC open-order snapshots, positions, account balance, and
live PnL. DTC `Price1` is the stop trigger for Stop/Stop Limit and `Price2` is the
limit price for Stop Limit, as required by the official protocol. Market If
Touched and Limit If Touched are rejected locally because their notification
mapping is not unambiguous through the current upstream API. The bridge does not
advertise OCO, third-party MBO, historical orders/fills, or live-environment
trading.

Quantities must be positive whole contracts and are limited locally (one contract
by default). ES prices must be positive, finite, and aligned to the 0.25 tick.
Client order IDs are mandatory and cannot be reused for the lifetime of the
trading service. Market orders are rejected by default, and credentials/account
identifiers are read only from `.env` or environment variables. A process-global
random locally administered MAC is supplied to Rithmic; the host hardware MAC is
never queried or sent.

Read-only Paper checks can be run without placing an order:

```powershell
cargo test --test trading_live paper_trading_order_plant_discovers_account_and_cme_route -- --ignored --nocapture
cargo test --test trading_live paper_trading_pnl_plant_accepts_account_snapshot -- --ignored --nocapture
```

With a trading-enabled `dtc_server` already running, this additionally verifies
the read-only DTC wire path for account discovery, no-working-orders, flat/current
positions, and account balance:

```powershell
$env:DTC_TEST_ADDR='127.0.0.1:11099'
cargo test --test dtc_live paper_trading_account_position_balance_flow_through_dtc_wire -- --ignored --nocapture
```

The lifecycle tests actually place Paper orders and therefore additionally
require `RITHMIC_RUN_ORDER_LIFECYCLE_TEST=true`. They remain ignored by default
and must never be used against a live environment.

## Sierra Chart verification checklist

1. Start `dtc_server` and confirm it reports the intended contract and local
   address (default `127.0.0.1:11099`).
2. In Sierra Chart, configure a DTC Service connection to that host and port with
   Binary Encoding and no TLS for this localhost connection.
3. Open `SYMBOL-EXCHANGE` (for example `ESU6-CME`) and verify Chart and Time &
   Sales update without reconnect loops.
4. Open Trade DOM and verify bid/ask levels update as aggregated Market-by-Price,
   never as third-party MBO.
5. Verify Numbers Bars/Footprint, Volume Profile, and Delta respond to classified
   trades; request a chart reload to validate historical ticks/bars.
6. With trading disabled, confirm Sierra cannot submit orders. For Paper trading,
   enable the environment switches above, confirm the account/positions/balance,
   verify the Trade Window clearly identifies the Paper account, then manually
   test exactly one one-lot Limit order far away from the market, modify it by one
   tick, and cancel it. Confirm the Orders tab reaches Canceled and that Open
   Orders is empty afterward. Do not test Market, Stop, or Stop Limit until this
   basic lifecycle has been observed successfully in Sierra.

## Depth and heatmap performance

The bridge can accept the DTC 16-bit depth limit, but requesting thousands of
levels for several active contracts can saturate Sierra Chart's chart thread,
especially with **Market Depth Historical Graph** enabled. The bridge still keeps
the complete upstream DBO book when Sierra requests fewer display levels.

For a responsive setup:

- subscribe to 300–500 levels only on the contract whose deep book is being
  inspected, and use about 100 on other open contracts;
- set the heatmap study's **Maximum Levels to Display** to 20–100 rather than 0;
- set **Show Quantity Numbers** to `None` or `Only After Last Bar`;
- use **Combine Increment in Ticks** of 2–4 when individual ticks are not needed;
- load one day of historical market depth unless more is required; and
- use a 500–1000 ms chart update interval when lower latency is not essential.

The server logs every DTC depth subscription as `NumLevels` and reports the size
of its initial combined bid/ask snapshot. This is the authoritative way to verify
that a Sierra symbol setting reached the bridge.

## Troubleshooting

- **Immediate disconnect:** verify Binary Encoding, the port, and that TLS is
  disabled. Check the server's DTC message log for a rejected request.
- **Login timeout or access error:** close other software using the same trial
  Rithmic login, confirm the Demo endpoint, and run `rithmic_probe` again.
- **Only one or a few symbols:** reconnect after the initial catalog has loaded.
  A manually entered futures contract is resolved and appended to the cumulative
  catalog when Rithmic reference data recognizes it.
- **Symbol appears under Other:** DTC security definitions do not carry Sierra's
  proprietary category and rollover metadata. This does not prevent data use.
- **Historical download repeats or stops:** inspect Sierra's Message Log and the
  bridge's `Historical request`/`Historical response chunk` messages. Keep a
  positive history timeout disabled unless one is specifically required.
- **Crossed book or repeated rebuild warning:** treat persistent warnings as a
  feed/book synchronization fault. The bridge withholds crossed L2 and requests a
  fresh DBO snapshot instead of publishing invalid depth.
- **High CPU:** temporarily remove Market Depth Historical Graph. If Sierra CPU
  falls, reduce subscribed and displayed depth before changing other settings.

## Project layout

```text
src/bin/rithmic_probe.rs  Upstream permission and live-data gate
src/bin/dtc_server.rs     Local server entry point
src/market_gateway.rs     Provider-neutral market/history/trading contracts
src/dtc.rs                DTC wire framing, encoding, and Sierra sessions
src/rithmic_feed.rs       Market plant supervision, catalog, subscriptions, DBO
src/order_book.rs         Full order book and Market-by-Price aggregation
src/history_feed.rs       History plant and bounded streaming requests
src/trading_feed.rs       Paper-only orders, positions, balances, and PnL
src/options.rs            Independent option collection, analytics, persistence, HTTP
src/identity.rs           Process-local synthetic MAC identity
tests/                    Credentialed ignored end-to-end checks
```

## Known limitations

- Phase-one production scope remains futures-focused. Equities are not advertised
  until search, entitlement, subscription, and actual real-time data are verified.
- Sierra-native MBO is not used; the DTC client receives aggregated L2 only.
- Historical market depth is not backfilled by this bridge. Sierra records the
  real-time L2 stream locally for its historical depth graph.
- Trading is standalone-order, Paper-only, and opt-in. OCO/attached-order
  semantics, historical fills, and live trading are not advertised.
- The order-price validator supports 0.25-tick ES/NQ-style contracts. Submissions
  for other tick sizes are explicitly rejected before reaching Rithmic.
- Session statistics include available open/high/low, cumulative volume, open
  interest, and settlement data. Total session trade count and trading-session
  date remain unset when upstream does not provide them.
- Exchange/underlying enumeration queries permitted futures contracts. Definition
  definitions include upstream expiration dates and exchange symbols when available.
  Definition subscriptions do not yet push later metadata changes; rollover,
  margin, and delayed-data metadata are not fully mapped.
- A Rithmic trial account may reject simultaneous sessions from another platform.

The [DTC audit fix record](docs/dtc-audit/fixes.md) tracks the September 2026
protocol fixes, offline regressions, and remaining implementation limits.
