# polyscalp-live

Every 10 minutes GitHub Actions records the real order book (top 5 bids/asks) of every open
Polymarket city-temperature market, plus the latest airport METAR reports.
No wallet, no keys, no trading — public market data only.

Data: `data/YYYY-MM-DD/books-HHMM.jsonl.gz` and `metar-HHMM.jsonl.gz` (gzip JSON lines).

## Forecast streams

Hourly, at each market's resolution station:

- `ensemble.mjs` → `data/ensemble/YYYY-MM-DD.jsonl` — ECMWF IFS 51-member ensemble,
  reduced to per-member daily max/min.
- `multisource.mjs` → `data/multisource/YYYY-MM-DD.jsonl` — the mode (most recurring
  value) across wttr.in, MET Norway, 7Timer and NOAA NWS (US cities).

Both workflows self-chain like the book logger (each run stays alive ~5h40m and
dispatches its successor; the hourly cron is only a backup restart). Before
2026-11-15 they relied on GitHub's scheduled triggers alone, which fired only
~4 times a day — see the CADENCE FIX note in the workflow files.

## Resting-fill paper probe

`probe.mjs` → `data/probe/YYYY-MM-DD.jsonl` (state in `data/probe/state.json`),
driven by `probe.yml` on the same 10-minute self-chaining rhythm.

PAPER ONLY — no wallet, no keys, no real orders. From 14:00 station-local time
the probe rests a pretend $5 SELL order at the best ask of the bucket holding
each lowest-temperature market's running METAR minimum (the "certainty-seller"
setup), then measures whether it would actually have been filled, using real
public trade prints (Polymarket Data API v2) — never book movements, which
cannot distinguish a fill from other sellers repricing. Fill rule, in brief:
only taker-BUY prints on the bucket's YES token at or above the rest price
count, and their volume must first exceed the ask size that was already
queued at that price when the order was placed; NO-side prints are ignored,
so the rule understates fills rather than flattering them. At settlement
(station METAR, routine reports, T-group tenths — the markets' own rules)
each row records held/broke and the paper PnL of the filled part. The full
rule and its reasoning are in the header of `probe.mjs`.

Why it exists: backtests put this strategy anywhere between −$6.57 and
+$28.43 on the same setups depending on the fill assumption, and snapshots
cannot tell which world is real. The probe is the measurement that settles
it — the first settled rows appear the day after it starts running.
