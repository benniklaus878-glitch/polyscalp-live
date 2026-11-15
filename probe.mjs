/**
 * probe.mjs — PolyScalp certainty-seller RESTING-FILL PAPER PROBE
 * (per MODEL_TRADER_SCOPE.md piece 4c and the 2026-11-15 strategy hunt).
 *
 * PAPER ONLY. No wallet, no keys, no real orders are ever placed. The probe
 * rests *pretend* sell orders on the live Polymarket book and measures the
 * one thing book snapshots cannot show: when a resting sell order sits on
 * an overpriced near-certain bucket, does it actually get filled — and if
 * so, on the buckets that go on to break (profit) or the ones that hold
 * (loss)? That fill question is the whole value of the certainty-seller
 * pattern; the 2026-11-15 hunt put the honest range at −$6.57 (strict
 * book proxy) to +$28.43 (100% fill) on the same 14 setups.
 *
 * STRATEGY (identical setup to the hunted "lows14 seller"):
 *   For each lowest-temperature market, from 14:00 station-local time,
 *   using only METAR reports stamped at or before the latest book
 *   snapshot: take the bucket holding the day's running minimum. If its
 *   best ask is in (0, 0.97] and its best bid is >= 0.01 (a genuine
 *   two-sided book), rest a $5 paper SELL order at that best ask.
 *   One order per market, at the first qualifying snapshot.
 *
 * FILL RULE (conservative, print-based — read this before quoting numbers):
 *   Fills are measured from REAL public trade prints (Polymarket Data API
 *   v2, GET /v2/trades?condition=<conditionId>), never from book changes:
 *   a book level disappearing cannot distinguish "we were lifted" from
 *   "other sellers repriced", so book movement is not evidence of a fill.
 *   A print counts toward our fill only if ALL of these hold:
 *     - it is on our bucket's YES token (token_id match),
 *     - its taker side is BUY (only taker buys consume resting asks),
 *     - its price is >= our rest price (it traded at or through our level),
 *     - its timestamp is after our placement snapshot and no later than
 *       2h after the station-local day ends (the order's resting life).
 *   Queue honesty: the visible ask size at our exact price in the
 *   placement snapshot was there before us (time priority), so eligible
 *   print volume must first exceed that queue-ahead size before any of
 *   our shares count as filled:
 *       filled_qty = clamp(cum_eligible_volume − queue_ahead, 0, qty)
 *   NO-side prints are ignored entirely (a NO buy is a YES sell into bids,
 *   which does not lift a YES ask in the print record we can see), so this
 *   rule UNDERSTATES fills rather than flattering them. Partial fills are
 *   recorded as partial; paper PnL is computed on the filled part only.
 *
 * SETTLEMENT: after the station-local day ends (+1h grace for late METAR),
 * the day's final running minimum is computed from routine METAR only
 * (SPECI excluded, precise T-group tenths — the market's own resolution
 * rules, same as the tester). Outcome "held" if our bucket is the final
 * winner, else "broke". Paper PnL of a filled sell of Q shares at price P:
 * proceeds Q·P minus payout Q·$1 if held, minus 0 if broke. If fewer than
 * 3 routine reports exist for the day even 26h after day end, the outcome
 * is recorded as "unknown" with no PnL — never guessed.
 *
 * TIMEZONES: station-local days use the same fixed November UTC offsets
 * as tester.py (the hunt's ground truth). Known simplification: offsets
 * are fixed, so a DST change mid-run would shift local hours for the
 * affected cities until the table is updated.
 *
 * MODES:
 *   node probe.mjs                      one live cycle (GitHub Actions chain)
 *   node probe.mjs --replay <dataDir> <outDir>
 *       Offline dry-run over logged books+METAR in <dataDir>: walks every
 *       snapshot chronologically, places the same paper orders, fetches
 *       the real historical trade prints for the placed markets, settles
 *       from logged METAR, writes rows to <outDir>/probe-replay.jsonl and
 *       prints a summary. Used to validate the probe before deployment.
 *
 * STATE (live mode): data/probe/state.json (open orders + market cache),
 * committed by probe.yml alongside the output rows so the stateless
 * Actions runners keep continuity. Final rows: data/probe/YYYY-MM-DD.jsonl
 * (one row per settled order, keyed by the market's station-local date).
 */
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import axios from 'axios';

const GAMMA = 'https://gamma-api.polymarket.com';
const DATA_API = 'https://data-api.polymarket.com/v2';
const STAKE = 5.0;               // paper dollars per order
const GATE_HOUR = 14;            // station-local hour gate (lows14)
const MAX_ASK = 0.97;            // setup requires best ask in (0, 0.97]
const MIN_BID = 0.01;            // and a genuine bid >= 0.01
const MIN_REPORTS = 3;           // routine METAR reports before signalling
const FILL_GRACE_SEC = 2 * 3600; // order rests until day end + 2h
const SETTLE_GRACE_SEC = 3600;   // settle 1h after local day end
const SNAPSHOT_MAX_AGE = 1800;   // never place on a snapshot older than 30m
const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];
const CITIES = JSON.parse(fs.readFileSync('cities.json', 'utf8'));

// Station-local UTC offsets (hours) for November — identical to tester.py.
const OFFSETS = {
  'amsterdam': 1, 'ankara': 3, 'atlanta': -5, 'austin': -6, 'beijing': 8,
  'buenos-aires': -3, 'busan': 9, 'cape-town': 2, 'chengdu': 8, 'chicago': -6,
  'chongqing': 8, 'dallas': -6, 'denver': -7, 'guangzhou': 8, 'helsinki': 2,
  'hong-kong': 8, 'houston': -6, 'istanbul': 3, 'jeddah': 3, 'jinan': 8,
  'karachi': 5, 'kuala-lumpur': 8, 'london': 0, 'los-angeles': -8,
  'lucknow': 5.5, 'madrid': 1, 'manila': 8, 'mexico-city': -6, 'miami': -5,
  'milan': 1, 'moscow': 3, 'munich': 1, 'nyc': -5, 'panama-city': -5,
  'paris': 1, 'qingdao': 8, 'san-francisco': -8, 'sao-paulo': -3,
  'seattle': -8, 'seoul': 9, 'shanghai': 8, 'shenzhen': 8, 'singapore': 8,
  'taipei': 8, 'tel-aviv': 2, 'tokyo': 9, 'toronto': -5, 'warsaw': 1,
  'wellington': 13, 'wuhan': 8, 'zhengzhou': 8,
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const iso = (ts) => new Date(ts * 1000).toISOString();
const clamp = (x, lo, hi) => Math.min(hi, Math.max(lo, x));

async function get(url, opts = {}, tries = 4) {
  for (let i = 0; i < tries; i++) {
    try { return (await axios({ url, timeout: 30000, ...opts })).data; }
    catch (e) {
      const st = e.response?.status;
      if (st === 404 || st === 400) return null;
      if (i === tries - 1) throw e;
      await sleep(1500 * (i + 1) + (st === 429 ? 5000 : 0));
    }
  }
}

// ─── slug / bucket / time helpers (ported from tester.py) ───────────────────
const SLUG_RE = /^(highest|lowest)-temperature-in-(.+)-on-([a-z]+)-(\d+)-(\d+)$/;
function parseSlug(slug) {
  const m = SLUG_RE.exec(slug);
  if (!m) return null;
  return { kind: m[1], city: m[2], ymd: [Number(m[5]), MONTHS.indexOf(m[3]) + 1, Number(m[4])] };
}
function localDayWindow(city, ymd) {
  const off = OFFSETS[city];
  if (off === undefined) return null;
  const start = Date.UTC(ymd[0], ymd[1] - 1, ymd[2]) / 1000 - off * 3600;
  return [start, start + 86400];
}
function localHour(city, ts) {
  const off = OFFSETS[city] ?? 0;
  return Math.floor(((ts + off * 3600) % 86400) / 3600);
}
const BUCKET_RE = /^(-?\d+)°([CF])( or below| or higher)?$/;
const RANGE_RE = /^(-?\d+)-(-?\d+)°([CF])$/;
function bucketRange(label) {
  const s = String(label).trim();
  let m = RANGE_RE.exec(s);
  if (m) return [Number(m[1]), Number(m[2]), m[3]];
  m = BUCKET_RE.exec(s);
  if (!m) return null;
  const n = Number(m[1]);
  if (m[3] === ' or below') return [-Infinity, n, m[2]];
  if (m[3] === ' or higher') return [n, Infinity, m[2]];
  return [n, n, m[2]];
}
function winnerBucket(buckets, valueC) {
  // buckets: {label: [lo, hi, unit]}; valueC: precise temp in °C
  const labels = Object.keys(buckets);
  if (!labels.length || valueC === null || valueC === undefined) return null;
  const unit = buckets[labels[0]][2];
  const v = Math.floor(unit === 'F' ? valueC * 9 / 5 + 32 : valueC);
  for (const [label, [lo, hi]] of Object.entries(buckets)) if (lo <= v && v <= hi) return label;
  return null;
}

// ─── METAR (ported from tester.py: routine only, T-group tenths) ────────────
const TGROUP_RE = /\bT([01])(\d{3})([01])(\d{3})\b/;
function preciseTempC(row) {
  const raw = row.raw || '';
  const t = TGROUP_RE.exec(raw);
  if (t) { const v = Number(t[2]) / 10; return t[1] === '1' ? -v : v; }
  if (row.temp_c !== null && row.temp_c !== undefined) { const v = Number(row.temp_c); if (!Number.isNaN(v)) return v; }
  if (row.temp_f !== null && row.temp_f !== undefined) { const v = Number(row.temp_f); if (!Number.isNaN(v)) return (v - 32) * 5 / 9; }
  return null;
}
function obsEpoch(row) {
  const o = row.obs;
  if (typeof o === 'number') return Math.floor(o);
  if (typeof o === 'string' && /^\d+$/.test(o)) return Number(o);
  if (typeof o === 'string') { const t = Date.parse(o.replace(' ', 'T') + (o.includes('T') ? '' : 'Z')); if (!Number.isNaN(t)) return Math.floor(t / 1000); }
  return null;
}
function readGzLines(file) {
  try { return zlib.gunzipSync(fs.readFileSync(file)).toString().split('\n').filter((l) => l.trim()); }
  catch { return []; }
}
function listDataFiles(dataDir, kind, maxDaysBack = null) {
  const days = fs.readdirSync(dataDir).filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d)).sort();
  const use = maxDaysBack ? days.slice(-maxDaysBack) : days;
  const out = [];
  for (const d of use) for (const f of fs.readdirSync(path.join(dataDir, d)).sort())
    if (f.startsWith(kind + '-') && f.endsWith('.jsonl.gz')) out.push(path.join(dataDir, d, f));
  return out;
}
// station -> sorted [{ep, tc, routine}], deduped by (station, epoch)
function loadMetar(dataDir, maxDaysBack = null) {
  const seen = new Map();
  for (const f of listDataFiles(dataDir, 'metar', maxDaysBack)) {
    for (const line of readGzLines(f)) {
      let r; try { r = JSON.parse(line); } catch { continue; }
      const ep = obsEpoch(r); const tc = preciseTempC(r);
      if (!r.station || ep === null || tc === null) continue;
      const key = r.station + '|' + ep;
      if (!seen.has(key)) seen.set(key, { ep, tc, routine: !(r.raw || '').startsWith('SPECI') });
    }
  }
  const by = new Map();
  for (const [key, v] of seen) {
    const st = key.split('|')[0];
    if (!by.has(st)) by.set(st, []);
    by.get(st).push(v);
  }
  for (const rows of by.values()) rows.sort((a, b) => a.ep - b.ep);
  return by;
}
function runningMin(metar, station, window, uptoTs) {
  const rows = metar.get(station) || [];
  let mn = null, n = 0;
  for (const r of rows) {
    if (r.ep < window[0]) continue;
    if (r.ep > uptoTs || r.ep >= window[1]) break;
    if (!r.routine) continue;
    n++; if (mn === null || r.tc < mn) mn = r.tc;
  }
  return { mn, n };
}
function dayStats(metar, station, window) {
  const rows = metar.get(station) || [];
  let mn = null, n = 0, first = null, last = null;
  for (const r of rows) {
    if (r.ep < window[0]) continue;
    if (r.ep >= window[1]) break;
    if (!r.routine) continue;
    n++; if (mn === null || r.tc < mn) mn = r.tc;
    if (first === null) first = r.ep; last = r.ep;
  }
  return { mn, n, spanH: first !== null && last !== null ? (last - first) / 3600 : 0 };
}

// ─── market discovery (Gamma; lowest-temperature only) ──────────────────────
function stationFromRules(text = '', source = '') {
  const awc = String(source).match(/[?&]ids=([A-Z0-9]{4})/i) || text.match(/aviationweather\.gov[^\s"'<>]*?ids=([A-Z0-9]{4})/i);
  if (awc) return awc[1].toUpperCase();
  const paren = text.match(/\(([A-Z]{4})\)/);
  if (paren) return paren[1];
  const wu = text.match(/wunderground\.com\/history\/daily\/(?:[^.\s"'<>]+)\/([a-z0-9]{3,5})/i);
  if (wu) return wu[1].toUpperCase();
  const noaa = text.match(/weather\.gov[^\s"'<>]*?site=([a-z0-9]{3,5})/i);
  if (noaa) return noaa[1].toUpperCase();
  return null;
}
const parseArr = (x) => { if (Array.isArray(x)) return x; try { return JSON.parse(x || '[]'); } catch { return []; } };
async function discoverLowest() {
  const now = Date.now();
  const slugs = [];
  for (const k of [-1, 0, 1]) {
    const d = new Date(now + k * 86400e3);
    for (const c of CITIES) slugs.push(`lowest-temperature-in-${c}-on-${MONTHS[d.getUTCMonth()]}-${d.getUTCDate()}-${d.getUTCFullYear()}`);
  }
  const out = {};
  let i = 0;
  await Promise.all(Array.from({ length: 8 }, async () => {
    while (i < slugs.length) {
      const slug = slugs[i++];
      try {
        const ev = await get(`${GAMMA}/events/slug/${slug}`);
        if (!ev?.markets?.length || ev.closed) continue;
        const buckets = ev.markets.map((m) => {
          const t = parseArr(m.clobTokenIds);
          return { outcome: m.groupItemTitle || m.question, yes: t[0], no: t[1], conditionId: m.conditionId, closed: !!m.closed };
        }).filter((b) => b.yes && b.conditionId);
        if (!buckets.length || buckets.every((b) => b.closed)) continue;
        out[slug] = { station: stationFromRules(ev.description || '', ev.resolutionSource || ''), buckets };
      } catch { /* skip */ }
    }
  }));
  return out;
}
async function fetchConditionIds(slug) {
  // replay helper: slug -> {bucketLabel: conditionId}
  const ev = await get(`${GAMMA}/events/slug/${slug}`);
  const map = {};
  if (ev?.markets) for (const m of ev.markets) map[m.groupItemTitle || m.question] = m.conditionId;
  return map;
}

// ─── trade prints + the fill rule ───────────────────────────────────────────
async function fetchTrades(conditionId, sinceTs) {
  // Data API v2 pagination is cursor-only: an `offset` param is rejected
  // with HTTP 400, and the next page is requested with `cursor` set to the
  // previous page's pagination.next_cursor. Errors are allowed to throw —
  // a silently-empty trade list would fake "no fills" (that bug bit the
  // first dry-run: the shared get() helper swallows 400s as null).
  const out = [];
  const limit = 500;
  let cursor = null;
  for (let page = 0; page < 20; page++) {
    const params = { condition: conditionId, limit };
    if (cursor) params.cursor = cursor;
    const { data } = await axios.get(`${DATA_API}/trades`, { params, timeout: 30000 });
    const rows = data?.data || [];
    if (!rows.length) break;
    for (const r of rows) out.push({ ts: Number(r.timestamp), price: Number(r.price), size: Number(r.size), tokenId: String(r.token_id), side: r.side });
    const oldest = Math.min(...rows.map((r) => Number(r.timestamp)));
    cursor = data?.pagination?.next_cursor || null;
    if (!data?.pagination?.has_more || !cursor || oldest <= sinceTs) break;
    await sleep(150);
  }
  return out.filter((t) => t.ts > sinceTs).sort((a, b) => a.ts - b.ts);
}
function applyFillRule(order, trades) {
  // Recompute from scratch each call (idempotent): eligible prints only.
  let cum = 0, fillTs = null;
  const cutoff = order.window[1] + FILL_GRACE_SEC;
  for (const t of trades) {
    if (t.ts <= order.restTs || t.ts > cutoff) continue;
    if (t.tokenId !== String(order.tokenYes) || t.side !== 'BUY') continue;
    if (t.price < order.restPrice - 1e-9) continue;
    const before = clamp(cum - order.queueAhead, 0, order.qty);
    cum += t.size;
    const after = clamp(cum - order.queueAhead, 0, order.qty);
    if (before <= 0 && after > 0 && fillTs === null) fillTs = t.ts;
    if (after >= order.qty) break;
  }
  order.eligibleVolume = cum;
  order.filledQty = clamp(cum - order.queueAhead, 0, order.qty);
  order.filled = order.filledQty > 1e-9;
  if (fillTs !== null) order.fillTs = fillTs;
}

// ─── setup evaluation (the hunted lows14-seller setup, verbatim gates) ──────
function evaluateSetup({ slug, station, bucketsMeta, bookRow, metar, snapTs }) {
  const p = parseSlug(slug);
  if (!p || p.kind !== 'lowest' || !station) return null;
  const window = localDayWindow(p.city, p.ymd);
  if (!window || !(window[0] <= snapTs && snapTs < window[1])) return null;
  if (localHour(p.city, snapTs) < GATE_HOUR) return null;
  const { mn, n } = runningMin(metar, station, window, snapTs);
  if (n < MIN_REPORTS || mn === null) return null;
  const ranges = {};
  for (const b of bucketsMeta) { const r = bucketRange(b.outcome); if (r) ranges[b.outcome] = r; }
  const label = winnerBucket(ranges, mn);
  if (!label || !bookRow || bookRow.bucket !== label) return null;
  const asks = bookRow.asks || [], bids = bookRow.bids || [];
  if (!asks.length || !bids.length) return null;
  const ask = asks[0][0], bid = bids[0][0];
  if (!(ask > 0) || ask > MAX_ASK || bid < MIN_BID) return null;
  const meta = bucketsMeta.find((b) => b.outcome === label);
  if (!meta) return null;
  return {
    id: `${slug}|${label}`,
    slug, city: p.city, station, localDate: `${p.ymd[0]}-${String(p.ymd[1]).padStart(2, '0')}-${String(p.ymd[2]).padStart(2, '0')}`,
    bucket: label, tokenYes: meta.yes, conditionId: meta.conditionId || null,
    window, buckets: ranges,
    restTs: snapTs, restPrice: ask, entryBid: bid,
    qty: STAKE / ask, stake: STAKE,
    queueAhead: asks[0][1] || 0,
    cumEligible: 0, eligibleVolume: 0, filledQty: 0, filled: false, fillTs: null,
    status: 'open',
  };
}

function settleOrder(order, metar) {
  const { mn, n, spanH } = dayStats(metar, order.station, order.window);
  const winner = n >= 1 && mn !== null ? winnerBucket(order.buckets, mn) : null;
  let outcome = 'unknown', pnl = null;
  if (winner !== null && n >= MIN_REPORTS) {
    outcome = winner === order.bucket ? 'held' : 'broke';
    const proceeds = order.filledQty * order.restPrice;
    pnl = order.filledQty > 0 ? proceeds - (outcome === 'held' ? order.filledQty : 0) : 0;
  }
  return {
    id: order.id, city: order.city, date: order.localDate, slug: order.slug,
    station: order.station, bucket: order.bucket,
    rest_time: iso(order.restTs), rest_ts: order.restTs, rest_price: order.restPrice,
    entry_bid: order.entryBid, size_usd: order.stake,
    qty_shares: Number(order.qty.toFixed(6)), queue_ahead_shares: order.queueAhead,
    eligible_print_volume: Number((order.eligibleVolume || 0).toFixed(6)),
    filled: !!order.filled, filled_qty: Number((order.filledQty || 0).toFixed(6)),
    fill_time: order.fillTs ? iso(order.fillTs) : null, fill_ts: order.fillTs || null,
    outcome, final_min_c: mn, winner_bucket: winner,
    metar_reports: n, metar_span_h: Number(spanH.toFixed(2)),
    paper_pnl_usd: pnl === null ? null : Number(pnl.toFixed(6)),
  };
}

// ─── live cycle ─────────────────────────────────────────────────────────────
const PROBE_DIR = 'data/probe';
const STATE_FILE = `${PROBE_DIR}/state.json`;
function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); }
  catch { return { cacheTs: 0, markets: {}, orders: [], settledIds: [] }; }
}
function loggedRowIds() {
  const ids = new Set();
  if (!fs.existsSync(PROBE_DIR)) return ids;
  for (const f of fs.readdirSync(PROBE_DIR)) {
    if (!/^\d{4}-\d{2}-\d{2}\.jsonl$/.test(f)) continue;
    for (const line of fs.readFileSync(path.join(PROBE_DIR, f), 'utf8').split('\n')) {
      if (!line.trim()) continue;
      try { ids.add(JSON.parse(line).id); } catch { /* ignore */ }
    }
  }
  return ids;
}
function newestSnapshot(dataDir) {
  const files = listDataFiles(dataDir, 'books');
  if (!files.length) return null;
  const rows = [];
  for (const line of readGzLines(files[files.length - 1])) { try { rows.push(JSON.parse(line)); } catch { /* skip */ } }
  if (!rows.length) return null;
  const byKey = new Map(rows.map((r) => [r.slug + '|' + r.bucket, r]));
  return { ts: rows[0].ts, byKey };
}

async function liveCycle() {
  fs.mkdirSync(PROBE_DIR, { recursive: true });
  const state = loadState();
  const logged = loggedRowIds();
  state.orders = state.orders.filter((o) => !logged.has(o.id));
  state.settledIds = [...new Set([...(state.settledIds || []), ...logged])].slice(-500);
  const now = Math.floor(Date.now() / 1000);

  if (now * 1000 - (state.cacheTs || 0) > 2 * 3600e3 || !Object.keys(state.markets || {}).length) {
    state.markets = await discoverLowest();
    state.cacheTs = Date.now();
    log(`discovered ${Object.keys(state.markets).length} open lowest-temperature markets`);
  }
  const metar = loadMetar('data', 4);
  const snap = newestSnapshot('data');
  let placed = 0, updated = 0, settled = 0;

  // 1) placements from the newest snapshot
  if (snap && now - snap.ts <= SNAPSHOT_MAX_AGE) {
    const have = new Set([...state.orders.map((o) => o.slug), ...state.settledIds.map((id) => id.split('|')[0])]);
    for (const [slug, mkt] of Object.entries(state.markets)) {
      if (have.has(slug)) continue;
      // find this market's running-min bucket row in the snapshot, if present
      const p = parseSlug(slug);
      if (!p || !mkt.station) continue;
      const window = localDayWindow(p.city, p.ymd);
      if (!window || !(window[0] <= snap.ts && snap.ts < window[1])) continue;
      if (localHour(p.city, snap.ts) < GATE_HOUR) continue;
      const { mn, n } = runningMin(metar, mkt.station, window, snap.ts);
      if (n < MIN_REPORTS || mn === null) continue;
      const ranges = {};
      for (const b of mkt.buckets) { const r = bucketRange(b.outcome); if (r) ranges[b.outcome] = r; }
      const label = winnerBucket(ranges, mn);
      if (!label) continue;
      const order = evaluateSetup({ slug, station: mkt.station, bucketsMeta: mkt.buckets, bookRow: snap.byKey.get(slug + '|' + label), metar, snapTs: snap.ts });
      if (order) { state.orders.push(order); placed++; log(`REST ${order.city} ${order.localDate} ${order.bucket} @ ${order.restPrice} (bid ${order.entryBid}, queue ${order.queueAhead})`); }
    }
  } else if (snap) {
    log(`newest snapshot is ${Math.round((now - snap.ts) / 60)}m old — skipping placements this cycle`);
  }

  // 2) fill updates + 3) settlements
  const stillOpen = [];
  for (const order of state.orders) {
    if (order.conditionId) {
      try { applyFillRule(order, await fetchTrades(order.conditionId, order.restTs)); updated++; }
      catch (e) { log(`trades fetch failed for ${order.id}: ${e.message}`); }
    }
    const dayEnd = order.window[1];
    if (now >= dayEnd + SETTLE_GRACE_SEC) {
      const { n } = dayStats(metar, order.station, order.window);
      const tooOld = now >= dayEnd + 26 * 3600;
      if (n >= MIN_REPORTS || tooOld) {
        const row = settleOrder(order, metar);
        fs.appendFileSync(path.join(PROBE_DIR, `${order.localDate}.jsonl`), JSON.stringify(row) + '\n');
        state.settledIds.push(order.id);
        settled++;
        log(`SETTLED ${row.id} outcome=${row.outcome} filled=${row.filled} qty=${row.filled_qty} pnl=${row.paper_pnl_usd}`);
        continue;
      }
    }
    stillOpen.push(order);
  }
  state.orders = stillOpen;
  fs.writeFileSync(STATE_FILE, JSON.stringify(state));
  log(`probe cycle done: placed=${placed} fill-updates=${updated} settled=${settled} open=${state.orders.length}`);
}

// ─── replay (offline dry-run over logged data) ──────────────────────────────
async function replay(dataDir, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const metar = loadMetar(dataDir);
  // Market metadata: station from the repo's stations.json (city -> station),
  // buckets/tokens/conditionIds from Gamma (cached in outDir). Deliberately
  // NOT the logger's markets.json.gz: that cache only holds currently-open
  // markets, so past days' markets would be missing from it.
  const stationsPath = [path.join(path.dirname(dataDir), 'stations.json'), 'stations.json'].find((p) => fs.existsSync(p));
  const stations = stationsPath ? JSON.parse(fs.readFileSync(stationsPath, 'utf8')) : {};
  const gammaCacheFile = path.join(outDir, 'gamma-cache.json');
  let gammaCache = {};
  try { gammaCache = JSON.parse(fs.readFileSync(gammaCacheFile, 'utf8')); } catch { /* fresh */ }
  async function gammaMeta(slug) {
    if (gammaCache[slug]) return gammaCache[slug];
    const ev = await get(`${GAMMA}/events/slug/${slug}`);
    let meta = null;
    if (ev?.markets?.length) {
      meta = {
        station: stationFromRules(ev.description || '', ev.resolutionSource || ''),
        buckets: ev.markets.map((m) => {
          const t = parseArr(m.clobTokenIds);
          return { outcome: m.groupItemTitle || m.question, yes: t[0], no: t[1], conditionId: m.conditionId, closed: !!m.closed };
        }).filter((b) => b.yes && b.conditionId),
      };
    }
    gammaCache[slug] = meta;
    fs.writeFileSync(gammaCacheFile, JSON.stringify(gammaCache));
    return meta;
  }
  const files = listDataFiles(dataDir, 'books');
  log(`replay: ${files.length} snapshots`);
  const orders = new Map();       // slug -> order
  const finalBid = new Map();     // slug -> {bucket: bestBid}
  let lastTs = 0;
  for (const f of files) {
    const rows = [];
    for (const line of readGzLines(f)) { try { rows.push(JSON.parse(line)); } catch { /* skip */ } }
    if (!rows.length) continue;
    const ts = rows[0].ts; lastTs = Math.max(lastTs, ts);
    const byKey = new Map(rows.map((r) => [r.slug + '|' + r.bucket, r]));
    const touched = new Set(rows.map((r) => r.slug));
    for (const r of rows) {
      if (!finalBid.has(r.slug)) finalBid.set(r.slug, {});
      finalBid.get(r.slug)[r.bucket] = r.bids?.length ? r.bids[0][0] : null;
    }
    for (const slug of touched) {
      if (orders.has(slug)) continue;
      const p = parseSlug(slug);
      if (!p || p.kind !== 'lowest') continue;
      const meta = await gammaMeta(slug);
      const station = stations[p.city]?.station || meta?.station || null;
      if (!station || !meta?.buckets?.length) continue;
      meta.station = station;
      const window = localDayWindow(p.city, p.ymd);
      if (!window || !(window[0] <= ts && ts < window[1])) continue;
      if (localHour(p.city, ts) < GATE_HOUR) continue;
      const { mn, n } = runningMin(metar, meta.station, window, ts);
      if (n < MIN_REPORTS || mn === null) continue;
      const ranges = {};
      for (const b of meta.buckets) { const r = bucketRange(b.outcome); if (r) ranges[b.outcome] = r; }
      const label = winnerBucket(ranges, mn);
      if (!label) continue;
      const order = evaluateSetup({ slug, station: meta.station, bucketsMeta: meta.buckets, bookRow: byKey.get(slug + '|' + label), metar, snapTs: ts });
      if (order) orders.set(slug, order);
    }
  }
  log(`replay: placed ${orders.size} paper orders`);
  // fills from the real historical prints
  for (const order of orders.values()) {
    if (!order.conditionId) {
      const cmap = await fetchConditionIds(order.slug).catch(() => ({}));
      order.conditionId = cmap[order.bucket] || null;
    }
    if (order.conditionId) {
      try { applyFillRule(order, await fetchTrades(order.conditionId, order.restTs)); }
      catch (e) { log(`trades fetch failed for ${order.id}: ${e.message}`); }
    }
    await sleep(120);
  }
  // settle what is settleable by the end of the logged data
  const rows = [];
  let pending = 0;
  for (const order of orders.values()) {
    if (!(order.window[1] + SETTLE_GRACE_SEC <= lastTs)) { pending++; continue; }
    const row = settleOrder(order, metar);
    // headline-scope flags, computed exactly like the strategy hunt
    const { n, spanH } = dayStats(metar, order.station, order.window);
    const fb = finalBid.get(order.slug) || {};
    let bookWin = null, best = -1;
    for (const [lab, v] of Object.entries(fb)) if (v !== null && v >= 0.9 && v > best) { best = v; bookWin = lab; }
    row.headline_scope = n >= 6 && spanH >= 8 && bookWin !== null && bookWin === row.winner_bucket;
    rows.push(row);
  }
  fs.writeFileSync(path.join(outDir, 'probe-replay.jsonl'), rows.map((r) => JSON.stringify(r)).join('\n') + (rows.length ? '\n' : ''));
  const settledRows = rows.filter((r) => r.outcome !== 'unknown');
  const hl = settledRows.filter((r) => r.headline_scope);
  const sum = (xs, f) => xs.reduce((a, r) => a + (f(r) || 0), 0);
  console.log(JSON.stringify({
    snapshots: files.length, placed: orders.size, settled: rows.length, pending,
    headline_scope_settled: hl.length,
    all: { filled: settledRows.filter((r) => r.filled).length, pnl: Number(sum(settledRows, (r) => r.paper_pnl_usd).toFixed(2)) },
    headline: { filled: hl.filter((r) => r.filled).length, held: hl.filter((r) => r.outcome === 'held').length, pnl: Number(sum(hl, (r) => r.paper_pnl_usd).toFixed(2)) },
  }, null, 1));
}

// ─── main ───────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
if (args[0] === '--replay') {
  const dataDir = args[1] || 'data';
  const outDir = args[2] || '/tmp/probe-replay-out';
  replay(dataDir, outDir).then(() => process.exit(0)).catch((e) => { console.error('replay failed:', e); process.exit(1); });
} else {
  liveCycle().then(() => process.exit(0)).catch((e) => { console.error('probe cycle failed:', e); process.exit(1); });
}
