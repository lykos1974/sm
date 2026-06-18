# PnF SaaS Platform Architecture

## Purpose

This document defines a practical target architecture for evolving the current PnF scanner, trader, market collector, research tooling, and Tkinter prototype into a future subscription-based SaaS platform.

This is a planning document only. It does not promote any trading experiment, change the current profitable baseline, alter strategy logic, or define a production launch approval.

## Non-goals

- Do not replace the existing Tkinter prototype immediately.
- Do not rewrite the PnF, structure, or strategy engines as part of the first step.
- Do not combine frontend UI concerns with trading logic.
- Do not enable production auto-trading for clients without explicit legal, compliance, risk, security, and operational review.
- Do not treat experimental research outputs as production trading state.

## 1. Current state

The repository is currently best understood as a trading research and prototype system with several useful components that should be separated before becoming a SaaS product.

### PnF engine

- Computes Point and Figure columns from market price data.
- Produces the chart-derived state that downstream structure, setup, scanner, and visualization workflows depend on.
- Should remain a deterministic backend computation module with no dependency on any web or desktop UI.

### Structure engine

- Interprets PnF output into higher-level structural context.
- Supports research around pullbacks, breakouts, structural states, and validation snapshots.
- Should remain separate from both raw market ingestion and user-facing presentation code.

### Strategy engine

- Evaluates strategy setups using PnF and structural features.
- Current protected interfaces and metric names should remain stable unless explicitly approved.
- The current stable rollback profile is long-only and should remain isolated from future experiments until a superior candidate is explicitly promoted.

### Live Binance trader

- Provides live exchange-facing trading capability for Binance-oriented workflows.
- Must be treated as an execution component, not as a frontend feature.
- Should depend on explicit strategy decisions, risk settings, execution configuration, and exchange credentials; it must not depend on Tkinter or future web UI state.

### Market collector DB

- Stores collected market data for research and live/prototype use.
- Acts as the source for replay, backfill, scanner computation, and chart generation.
- Future architecture should clarify which tables are raw immutable market data and which are derived outputs.

### Tkinter UI prototype

- Current local UI for operator workflows, scanner visualization, or prototype control.
- Should remain an internal tool during early SaaS migration.
- Should not become the SaaS frontend and should not be used as a dependency by backend trading services.

### Trade journal export

- Exports strategy or trade records for analysis, review, and journal workflows.
- Provides a natural starting point for future reporting and user-facing trade journal features.
- Future versions should preserve auditability: every exported trade should trace back to signal inputs, setup state, execution assumptions, and resolution data.

## 2. Target product vision

The target product is a web-based PnF market analysis platform that starts as a read-only scanner and charting SaaS, then adds alerts, account-specific settings, journals, and subscription tiers.

### Product capabilities

- **Web-based PnF platform:** Browser-accessible workspace for PnF charts, scanner results, setup context, and trade journal views.
- **Dynamic live charts:** Live-updating PnF charts derived from existing PnF columns and market collector data.
- **Scanner:** Symbol and timeframe scanner for structurally interesting PnF conditions and approved strategy setups.
- **Alerts:** User-configurable alerts for scanner matches, setup status changes, price levels, and strategy lifecycle events.
- **Strategy setups:** Read-only display of approved setup candidates, setup state, validation metadata, and risk/reward assumptions.
- **Trade journal:** Journal of simulated, paper, internal, or user-entered trades with export support.
- **User accounts:** Per-user authentication, watchlists, alert preferences, chart settings, and journal records.
- **Subscription tiers:** Commercial plans that gate feature access, symbol coverage, alert volume, historical depth, and advanced analytics.

### Suggested initial tier model

Keep pricing and entitlements simple until usage patterns are known:

- **Internal:** Full access for development, research, QA, and operations.
- **Free/watch:** Limited delayed scanner/chart access, small watchlist, no automation.
- **Pro:** Live charts, larger watchlists, alerts, scanner filters, journal exports.
- **Research/Advanced:** Deeper history, setup analytics, advanced filters, bulk exports, and beta research features.

## 3. Proposed architecture

The SaaS platform should be modular but not over-engineered. The first production shape can be a backend service, a worker process, a database, a web frontend, and a small number of queues or scheduled jobs.

```text
Exchange / Market Sources
        |
        v
Market Data Ingestion ---> Raw Market Data DB
        |                         |
        v                         v
PnF Computation Layer ----> Derived PnF Data
        |                         |
        v                         v
Structure + Signal Layer --> Signals / Setups
        |                         |
        +----> API Backend <------+----> Analytics / Reporting
                  |
                  +----> Web Frontend
                  +----> Notification Layer
                  +----> User/Auth/Subscription Layer
                  |
                  v
          Execution/Bot Layer
          internal only until reviewed
```

### Market data ingestion layer

Responsibilities:

- Collect live and historical candles/trades from Binance and future providers.
- Normalize exchange payloads into a consistent internal market data model.
- Persist raw data before any derived PnF or strategy computation.
- Support backfill, replay, and gap detection.

Boundaries:

- Does not evaluate strategy logic.
- Does not place trades.
- Does not know about UI sessions or user subscriptions except for possible entitlement-driven symbol coverage in later phases.

### PnF computation layer

Responsibilities:

- Convert raw market data into deterministic PnF columns and related chart state.
- Store derived PnF outputs with enough metadata to reproduce box size, reversal, symbol, timeframe, data window, and computation version.
- Provide chart-ready data to the API and scanner layers.

Boundaries:

- No direct frontend dependencies.
- No exchange credentials.
- No user-specific alert delivery.

### Signal/strategy layer

Responsibilities:

- Evaluate approved setup definitions against PnF and structure outputs.
- Register setup candidates, state transitions, validation records, and resolved outcomes.
- Maintain explicit risk parameters and strategy version metadata.

Boundaries:

- Research experiments remain separate from production-approved setup definitions.
- Public/protected strategy interfaces should remain stable unless explicitly changed.
- Signal output should be auditable and reproducible from stored inputs.

### Execution/bot layer

Responsibilities:

- Execute internal paper or live trades based on explicit approved signals and risk settings.
- Maintain exchange integration, order lifecycle state, fills, errors, retries, and kill-switch controls.
- Support internal operator workflows before any client-facing automation.

Boundaries:

- Must not depend on frontend runtime state.
- Must not ingest raw data directly when a normalized market data layer exists.
- Must not be offered as production auto-trading for clients until legal and operational review is complete.

### API backend

Responsibilities:

- Expose read-only chart, scanner, signal, setup, and journal endpoints first.
- Enforce authentication, authorization, feature entitlements, rate limits, and audit logging.
- Provide a stable contract between backend engines and web frontend.

Initial API scope:

- Symbols and timeframes.
- Latest PnF chart columns.
- Scanner results.
- Setup detail and lifecycle state.
- Trade journal rows and exports.

### Web frontend

Responsibilities:

- Browser-based charts, scanner tables, setup pages, alert configuration, and journal views.
- Render PnF columns using API data; do not recompute trading signals in the browser.
- Provide internal dashboards first, then beta user-facing views.

Boundaries:

- No trading logic in frontend code.
- No exchange keys in frontend code.
- No production execution controls without explicit backend authorization and review.

### User/auth/subscription layer

Responsibilities:

- User identity, sessions, roles, teams if needed, and account status.
- Subscription plan, entitlement checks, trial status, invoices, and webhook processing.
- Access control for symbols, timeframes, alerts, exports, historical depth, and beta features.

Recommended approach:

- Use a managed auth and billing provider initially unless there is a strong reason to self-host.
- Keep entitlement checks centralized in the API backend.

### Notification layer

Responsibilities:

- Deliver alert events through email, webhook, in-app notifications, and later SMS or messaging integrations.
- Deduplicate alerts and prevent noisy repeated notifications.
- Track notification delivery state for audit and support.

Boundaries:

- Notification workers receive signal or alert events; they do not compute trading setups.
- User alert preferences and subscription entitlements should be checked before delivery.

### Analytics/reporting layer

Responsibilities:

- Produce scanner performance summaries, setup outcome reports, trade journal analytics, and internal quality dashboards.
- Separate research metrics from production user analytics.
- Preserve scorecards and validation outputs for serious experiments.

Boundaries:

- Analytics jobs should not mutate live trading state.
- Research backtests should not overwrite production signal records.

## 4. Data architecture

The data model should separate raw observations, derived market structure, strategy decisions, user state, and audit records.

### Raw market data DB

Stores immutable or append-only market data:

- Exchange name.
- Symbol.
- Timeframe.
- Candle open/high/low/close/volume.
- Timestamp and ingestion timestamp.
- Source file, API cursor, or websocket metadata where applicable.
- Gap/backfill status.

### Derived PnF data

Stores reproducible chart outputs:

- Symbol and timeframe.
- Box size, reversal amount, price source, and computation version.
- PnF columns, column direction, high/low boxes, start/end timestamps.
- Derived annotations needed for rendering and downstream structure logic.

### Signals

Stores strategy-level outputs:

- Signal ID.
- Strategy name and version.
- Direction.
- Symbol and timeframe.
- Signal timestamp.
- Input PnF/structure references.
- Risk assumptions and invalidation levels.
- Signal status and lifecycle history.

### Setups

Stores setup candidates and validation records:

- Setup ID.
- Setup type.
- Registration timestamp.
- Pullback, breakout, structure, and quality fields used by approved strategies.
- Pending, active, resolved, discarded, or expired state.
- Resolution timestamp and outcome.
- Links to research scorecards when applicable.

### Trades

Stores execution and journal records:

- Trade ID.
- User or internal account ID.
- Optional signal/setup ID.
- Entry, stop, targets, size, fees, fills, exits, and realized R.
- Manual, paper, internal live, or future supported execution mode.
- Journal notes, tags, screenshots, and export metadata.

### User settings/watchlists

Stores user-specific product state:

- Watchlists.
- Default symbols and timeframes.
- Chart preferences.
- Scanner filters.
- Alert preferences.
- Journal display settings.

### Subscription data

Stores commercial access state:

- Plan ID.
- Provider customer ID.
- Subscription status.
- Trial and renewal dates.
- Entitlement snapshot.
- Webhook event IDs and processing status.

### Audit logs

Stores traceability records:

- User login and account changes.
- Subscription and entitlement changes.
- Alert creation, delivery, and suppression.
- Signal generation inputs and computation version.
- Trade execution decisions, risk checks, orders, fills, errors, and operator actions.

## 5. Migration plan from current repo

### Phase 0: keep Tkinter as internal prototype

- Continue using Tkinter for local/internal workflows.
- Add configuration clarity without changing trading logic.
- Avoid making Tkinter the foundation of the SaaS frontend.

Exit criteria:

- Operators know which database/settings file the prototype is using.
- Internal prototype workflows remain reproducible.

### Phase 1: stabilize backend engine and live data

- Document live market DB usage and expected table ownership.
- Clarify raw market data versus derived PnF/state outputs.
- Add operational checks for data freshness, gaps, and collector health.
- Preserve current strategy baseline and protected interfaces.

Exit criteria:

- Backend computations can run without Tkinter.
- Live data freshness can be checked from CLI or backend process logs.

### Phase 2: expose read-only API

- Build a small API around existing computed data.
- Start with read-only endpoints for symbols, PnF columns, scanner output, setup detail, and journal export rows.
- Do not expose execution controls.

Exit criteria:

- API can serve chart/scanner data to an internal client.
- API responses include enough IDs/version fields to audit derived data.

### Phase 3: build internal web dashboard

- Build a private web UI using the read-only API.
- Render PnF chart columns from backend-provided data.
- Add scanner tables and setup detail views.
- Keep Tkinter available as an internal fallback.

Exit criteria:

- Internal users can inspect charts and scanner state in a browser.
- No strategy logic is implemented in frontend code.

### Phase 4: add user accounts and alerts

- Add authentication and user settings.
- Add watchlists, scanner filters, and alert rules.
- Add notification delivery with deduplication and delivery logs.

Exit criteria:

- Alerts are tied to user accounts and entitlement checks.
- Alert deliveries are auditable.

### Phase 5: add subscriptions

- Add billing provider integration.
- Map plans to entitlements.
- Gate features by subscription status and plan.
- Add operational support workflows for failed payments, cancellations, and trials.

Exit criteria:

- Access control is enforced centrally in the API.
- Billing events are logged and replay-safe.

### Phase 6: beta launch

- Invite a small controlled beta group.
- Limit features to read-only scanner, charts, alerts, and journal workflows.
- Do not enable client production auto-trading.
- Monitor uptime, data correctness, alert quality, support load, and user behavior.

Exit criteria:

- Beta users can use the platform without local setup.
- Support and audit workflows are sufficient for a wider launch decision.

## 6. Guardrails

- Do not mix UI logic with trading logic.
- Trader must not depend on frontend runtime state.
- Market data ingestion must remain separate from trading execution.
- Research outputs must remain separate from live production state.
- Every signal and trade must be auditable from raw/derived inputs through decision and outcome.
- Risk management must remain explicit, versioned, and visible.
- No production auto-trading for clients until legal, compliance, risk, security, and operational review is complete.
- Preserve stable strategy interfaces and exported metric names unless a change is explicitly approved.
- Prefer small reversible migrations over large rewrites.
- Treat short-side logic and new experiments as unpromoted research unless explicitly promoted.

## 7. Recommended first implementation steps

These steps are intentionally modest and should not alter trading behavior.

1. **Add CLI db/settings overrides to Tkinter UI.**
   - Make it obvious which database and settings file are in use.
   - Avoid hard-coded local paths in operator workflows.
   - Keep this as a prototype usability improvement, not a SaaS dependency.

2. **Document live market DB usage.**
   - Identify raw market tables, derived tables, expected writers, expected readers, and replay/backfill assumptions.
   - Document how to verify data freshness and symbol coverage.

3. **Build read-only signal API prototype.**
   - Expose existing scanner/setup/signal information without write operations.
   - Include IDs, timestamps, strategy version, PnF computation metadata, and audit references.

4. **Build web chart prototype using existing PnF columns.**
   - Render backend-provided PnF columns in a simple browser view.
   - Avoid recomputing chart state in JavaScript.
   - Use this to validate API shape and frontend rendering needs.

5. **Add trade/setup overlays later.**
   - After base charts are reliable, overlay setup markers, signal state, entries, stops, targets, and resolved outcomes.
   - Keep overlays traceable to backend setup/trade IDs.

## 8. Open decisions

### Web stack choice

Options include a Python-first stack for speed of integration, a TypeScript frontend with a Python API, or a full TypeScript application with Python workers for computation. The practical default is to keep computation in Python and choose a frontend stack based on charting needs and team familiarity.

### Database choice

Decide whether the first SaaS backend uses the current database approach, PostgreSQL, TimescaleDB, DuckDB for analytics, or a split between operational and analytical stores. The first decision should optimize correctness, migrations, backups, and operational simplicity rather than theoretical scale.

### Hosting

Decide between a single VPS, managed containers, or cloud-managed services. Early beta should prioritize observability, backups, secure secrets, and simple deploy/rollback.

### Auth provider

Decide whether to use a managed auth provider or self-hosted auth. A managed provider is likely better initially unless data/control requirements say otherwise.

### Billing provider

Decide billing provider and entitlement model. Stripe is the default candidate, but the important architectural decision is to keep billing events replay-safe and entitlement checks centralized.

### Real-time delivery method

Options include polling, Server-Sent Events, WebSockets, or managed realtime infrastructure. Start with polling or SSE unless chart responsiveness and alert latency require WebSockets.

### Legal/compliance boundaries

Define what the product is allowed to provide: education, analytics, alerts, paper trading, internal live execution, or client execution. Production client auto-trading should remain out of scope until reviewed and explicitly approved.

## Practical north star

The safest path is to turn the current system into a backend-first, read-only web platform before adding accounts, alerts, billing, or execution features. The first SaaS milestone should prove that existing PnF columns, scanner outputs, and setup records can be served reliably through an auditable API and rendered in a browser without changing trading logic.
