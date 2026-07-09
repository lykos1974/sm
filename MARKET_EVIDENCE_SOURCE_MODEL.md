# Market Evidence Source Model

## Purpose

This document defines what an **Evidence Source** is inside the Research / Evidence Operating System.

The system must support many independent or semi-independent evidence producers without allowing any detector, dataset, or external feed to become an unvalidated final decision maker.

Potential evidence producers include:

- PnF structure
- Pole
- ABCD
- Harmonic
- Compression
- Funding
- Open Interest
- Liquidations
- Sentiment
- Order book
- On-chain
- Macro
- ETF flows
- Options / volatility
- News / events

This is an architecture note only. It does not add code, modify detectors, modify strategy, run research, create datasets, or change existing ROS documents.

## Core Principle

Detectors are not final decision makers.

Detectors are **Evidence Producers**.

An Evidence Producer may observe, transform, classify, summarize, or contextualize market information. It may produce features, candidates, warnings, confirmations, contradictions, or context. It does not decide whether a trade should be taken, whether an edge is validated, or whether production behavior should change.

Final decisions require validation, evidence history, decision records, and promotion rules.

---

## 1. Evidence Source

### What an Evidence Source Is

An **Evidence Source** is a named origin of market-relevant information that can contribute evidence to research or decision review.

It may be derived from:

- Price structure.
- Point-and-Figure structure.
- Pattern detectors.
- Derivatives data.
- Market positioning.
- Social or news sentiment.
- Order book state.
- On-chain behavior.
- Macro or flows data.
- Volatility or options data.
- Event calendars.

An Evidence Source is useful only if it can be described, traced, validated, monitored, and compared against actual outcomes.

### What an Evidence Source Is Not

An Evidence Source is not:

- A final trade decision.
- A strategy.
- A validated edge by default.
- A guarantee of independent information.
- A production promotion.
- A substitute for historical validation.
- A reason to override a protected baseline.

An Evidence Source may be promising, but it remains untrusted until validated.

### How It Differs From a Detector

A **detector** is an implementation that produces observations, labels, features, or candidate events.

An **Evidence Source** is the governed research identity of the information being produced.

Example:

- Detector: code that labels a compression pattern.
- Evidence Source: `compression_structure_context`, including origin, latency, lookahead risk, validation requirements, failure modes, confidence history, and current status.

Multiple detectors may serve one Evidence Source. One detector may also emit several features that belong to different Evidence Sources.

### How It Differs From a Feature

A **feature** is a structured observation or field.

An Evidence Source is broader. It owns the provenance and reliability context behind one or more features.

Example:

- Feature: `funding_rate_zscore`.
- Evidence Source: `perpetual_funding_context`.

The feature is a measurable value. The Evidence Source records why that value may matter, how it is sourced, what risks it has, and whether it has been validated.

### How It Differs From a Signal

A **signal** is an interpreted event or actionable-looking condition.

An Evidence Source is not inherently actionable. It may support, weaken, contradict, or contextualize a signal.

Example:

- Signal: `long_continuation_candidate`.
- Evidence Source: `open_interest_expansion_context` supporting or weakening the candidate.

Signals should not be treated as validated simply because an Evidence Source produced them.

### How It Differs From a Strategy

A **strategy** is a composed decision system that may use multiple Evidence Sources, risk rules, execution assumptions, and promotion decisions.

An Evidence Source is an input to research and decision review. It does not define complete entry, stop, target, sizing, execution, or portfolio behavior.

### How It Differs From an Edge

An **edge** is a validated belief with evidence that supports positive expected value under defined conditions.

An Evidence Source may become part of an edge, but it is not an edge by default.

Example:

- Evidence Source: `funding_extreme_context`.
- Edge component: funding extremes improve or degrade validated trade outcomes under a tested setup and market regime.

---

## 2. Evidence Source Contract

Every Evidence Source should be registered with a contract before it is treated as usable research infrastructure.

Required fields:

| Field | Required meaning |
|---|---|
| `source_id` | Stable unique identifier for the Evidence Source. |
| `source_type` | Category such as structure, pattern, derivatives, sentiment, order book, on-chain, macro, flows, volatility, or events. |
| `description` | Plain-language explanation of what information the source claims to provide. |
| `data origin` | Where the underlying data comes from, including exchange, vendor, internal detector, public feed, or manually curated source. |
| `update frequency` | How often the source can update, such as per candle, per tick, hourly, daily, weekly, or event-driven. |
| `latency` | Expected delay between market reality and source availability. |
| `historical availability` | Whether historical data exists, for which symbols / venues / time periods, and at what quality. |
| `survivorship risk` | Whether unavailable, delisted, filtered, or missing historical members may bias results. |
| `manipulation risk` | Whether the source can be spoofed, gamed, botted, revised, selectively reported, or distorted. |
| `lookahead risk` | Whether the source could accidentally use future information unavailable at decision time. |
| `correlation with existing sources` | Known or suspected overlap with already registered Evidence Sources. |
| `expected useful horizon` | Time horizon over which the source is expected to matter, such as immediate, intraday, multi-day, swing, or macro. |
| `market scope` | Symbols, venues, market families, asset classes, or regimes where the source is expected to apply. |
| `validation requirements` | Minimum evidence required before the source can influence decisions. |
| `failure modes` | Known ways the source may mislead research or decisions. |
| `confidence history` | Record of prior confidence, validation, counter-evidence, degradation, and review decisions. |
| `current status` | Current promotion stage, such as observed, candidate, validated context, eligible, or deprecated. |

### Contract Discipline

The contract is not a guarantee of usefulness. It is a control surface for reproducibility and decision quality.

A source without a contract should not be used to justify promotion, override baselines, or alter strategy behavior.

---

## 3. Independence / Double Counting

Evidence Sources must not be counted as independent unless their independence has been argued and tested.

Many sources describe the same underlying market pressure through different labels. Treating correlated sources as independent can create false confidence.

### Double Counting Examples

#### Twitter Sentiment and Social Volume

Twitter sentiment and social volume may not be independent.

A sudden increase in bullish posts and a sudden increase in social mentions may both reflect the same crowd behavior. Counting them separately as two independent confirmations can exaggerate confidence.

#### Funding and Long/Short Positioning

Funding and long/short positioning may overlap.

Both can reflect crowded derivatives positioning. If both are used, the ROS should record whether they are distinct enough to provide separate evidence or whether one is a proxy for the other.

#### PnF Breakout and Momentum

PnF breakout and momentum may describe the same price movement.

A breakout detector and a momentum feature can both be summaries of recent price expansion. If they are treated as independent, the system may double count price action.

#### News Sentiment and Price Reaction

News sentiment and price reaction may not be independent.

If a news event already caused price movement that appears in structure, then the structural source may already contain much of the news evidence. News sentiment may still matter, but it should be treated as context or contradiction unless validated otherwise.

### Independence Review Questions

Before combining Evidence Sources, ask:

1. Do these sources observe the same raw data?
2. Do they react to the same market participants?
3. Does one source lag the other?
4. Is one source derived from price while another merely explains price after the fact?
5. Do they fail at the same time?
6. Does historical validation show incremental value after controlling for the existing source?

If independence is unclear, classify the relationship as correlated or unresolved. Do not treat it as independent evidence.

---

## 4. Evidence Reliability

Evidence reliability should be tracked over time. Reliability is not static.

No source should be permanently trusted because it worked once, looked intuitive, or came from a respected data provider.

Reliability history should include:

### Initial Prior Confidence

Record the starting belief about the source before validation.

Examples:

- High prior because the data directly measures positioning.
- Medium prior because the source is plausible but noisy.
- Low prior because the source is untested or easy to manipulate.

Initial confidence is not validation. It is only a starting assumption.

### Validation Evidence

Track evidence that supports the source.

Examples:

- Improved trade outcomes when used as context.
- Better failure avoidance.
- Better regime identification.
- Stronger decision quality in out-of-sample review.

Validation evidence must remain tied to the experiments that produced it.

### Counter-Evidence

Track evidence that weakens the source.

Examples:

- No incremental value after controlling for price structure.
- High false positive rate.
- Strong performance in one period and failure in another.
- Apparent value explained by lookahead bias.

Counter-evidence should be preserved, not deleted.

### Regime Sensitivity

Record whether the source works only in certain conditions.

Examples:

- Funding extremes may matter during crowded leverage regimes.
- Macro data may matter more during rate-sensitive periods.
- Order book imbalance may degrade during low-liquidity or spoof-heavy conditions.

A source can be useful and still regime-limited.

### Degradation

A source may lose usefulness over time.

Reasons include:

- Market adaptation.
- Venue microstructure changes.
- Data vendor changes.
- Increased crowd awareness.
- Structural regime shifts.

Degradation should move the source toward review, downgrade, or deprecation.

### False Positives

Record when the source repeatedly supports bad decisions.

False positives are especially important for sources that appear persuasive but do not improve outcomes.

### Stale Data

Record whether the source becomes stale before decisions are made.

A stale sentiment snapshot, delayed funding print, or old macro release may be worse than no evidence if it creates false confidence.

### Source Drift

Source drift occurs when the meaning of a source changes over time.

Examples:

- A social platform changes its user base or bot activity.
- An exchange changes funding calculation rules.
- An API changes methodology.
- A news provider changes classification logic.

Source drift must be tracked because old validation may no longer apply.

---

## 5. Evidence Fusion

Evidence Fusion is the process of allowing multiple Evidence Sources to support, weaken, or contextualize a research or trade decision.

No single detector should be authoritative by default.

A decision may become stronger when several genuinely distinct sources support the same conclusion. A decision may become weaker when sources conflict, become stale, or duplicate the same information.

Evidence Fusion should answer questions such as:

- Does this source support the candidate setup?
- Does this source contradict the candidate setup?
- Does this source describe new information or repeat an existing source?
- Does this source apply at the same time horizon as the decision?
- Is this source validated for this market scope?
- Is this source currently reliable or degraded?
- Does this source introduce lookahead, latency, survivorship, or manipulation risk?

Evidence Fusion should not define weights in this document.

Evidence Fusion should not define machine learning in this document.

This architecture only establishes that multiple governed Evidence Sources can contribute to decisions without any detector becoming the final authority.

---

## 6. Sentiment Guidance

Sentiment should not initially be used as a primary signal.

Sentiment should be treated as a context / evidence source until validated against actual trade outcomes.

Social sentiment is noisy and may lag price. It may reflect crowd reaction after a move rather than predictive information before a move. It may also be distorted by bots, coordinated campaigns, influencer behavior, and platform-specific sampling bias.

### Preferred Order of Market Context

When available, prefer market-positioning evidence before social or news sentiment:

1. Funding.
2. Open Interest.
3. Liquidations.
4. Derivatives positioning.
5. Options / volatility context.
6. Social sentiment.
7. News sentiment.

This does not mean positioning data is always correct. It means positioning evidence is usually closer to actual market exposure than public commentary.

### How Sentiment May Be Useful

Social and news sentiment may be useful mainly:

- At extremes.
- As contradiction evidence.
- As context for crowdedness.
- As a warning that price structure may already reflect public information.
- As a regime or event annotation.

Examples:

- Extremely bullish social sentiment while structure weakens may be contradiction evidence.
- Panic news sentiment after a large move may mark exhaustion, but only if validated.
- Positive news sentiment after price has already expanded may be stale confirmation rather than new evidence.

### Sentiment Validation Requirement

Sentiment must be validated against actual trade outcomes before it can influence promotion or decision eligibility.

Validation should test whether sentiment adds incremental value beyond already available sources such as price structure, funding, OI, liquidations, and volatility context.

Until then, sentiment remains context only.

---

## 7. Promotion Rules

An Evidence Source may move through the following stages:

| Stage | Meaning |
|---|---|
| `OBSERVED` | The source has been noticed as potentially relevant but has no usable contract or dataset. |
| `CANDIDATE_SOURCE` | The source has a contract and a clear hypothesis but is not yet backed by usable historical data. |
| `DATASET_AVAILABLE` | Historical or live data is available with enough provenance to begin validation. |
| `VALIDATED_CONTEXT` | The source has been validated as useful context but not as a direct edge component. |
| `VALIDATED_EDGE_COMPONENT` | The source has demonstrated incremental value as part of a validated edge under defined conditions. |
| `DECISION_ENGINE_ELIGIBLE` | The source is eligible to influence decision logic under strict scope, monitoring, and rollback rules. |
| `DEPRECATED` | The source is retired, degraded, invalidated, too risky, or no longer worth using. |

### Promotion Discipline

Promotion is not automatic.

A source should not advance stages merely because data exists. It must satisfy the validation requirements appropriate to the next stage.

A source can move backward if counter-evidence, degradation, drift, or operational risk appears.

Deprecation should preserve the reason for failure so the same source is not repeatedly reintroduced without new evidence.

---

## 8. Anti-Waste Rules

The Evidence Operating System should prevent unnecessary data collection, duplicated research, and false confidence.

Rules:

1. Do not add external data before a clear hypothesis exists.
2. Do not trust sentiment without historical validation.
3. Do not combine correlated sources as independent evidence.
4. Do not allow any evidence source to override the protected Pole baseline without validation.
5. Preserve failed evidence sources with reason.
6. Do not promote a source because it is expensive, popular, novel, or intuitive.
7. Do not treat vendor documentation as validation.
8. Do not treat visual examples as evidence of edge.
9. Do not use a source if its latency makes it unavailable at decision time.
10. Do not hide degraded or failed source history.

These rules protect the research process from architecture bloat and unvalidated complexity.

---

## 9. Final Recommendation

After this document, architecture expansion should stop.

The next practical step should be one of:

1. Build ROS-1 minimal registries for Evidence Sources, ValidationRuns, Evidence, Decisions, Knowledge, ResearchDebt, Promotions, and RollbackPoints.
2. Migrate the Pole Core Motif into first usable registry records.

The project should not add more conceptual layers until there is a working registry that can preserve historical research, prevent repeated work, and improve decision quality.

The Evidence Source model should remain a governance model, not a trading model. It should make evidence safer to use without turning any detector, sentiment feed, or external dataset into an unvalidated final decision maker.
