/**
 * multisource.mjs — PolyScalp model-trader rig, piece 4 (companion to
 * ensemble.mjs; does NOT replace it).
 *
 * Purpose:
 *   The user's ORIGINAL PolyScalp bot took its entry signal from MORE THAN
 *   FIVE independent weather-forecast sources and used the value with the
 *   highest recurrence — the MODE — across them. That feed was never
 *   logged, so it could never be back-tested. This script logs an
 *   equivalent multi-source feed: every run, for every PolyScalp city, it
 *   asks each configured forecast source for its predicted daily MAX and
 *   MIN temperature at the exact station that city's Polymarket markets
 *   resolve on (coordinates from stations.json — the same table the
 *   ensemble logger uses), for station-local today and tomorrow, and
 *   records every source's values PLUS the mode of the maxes and the mode
 *   of the mins (rounded to whole degrees, as the original bot compared
 *   values), with the agreement count. After >=4 weeks of clean data the
 *   mode feed can finally be scored against reality (METAR) and against
 *   the market (order books), alongside the ECMWF ensemble feed.
 *   This script does NOT trade, place orders, or touch any wallet.
 *
 * ---------------------------------------------------------------------
 * PLUG-IN SOURCE LIST — how to swap / add / remove sources
 * ---------------------------------------------------------------------
 *   Sources live in the SOURCES array below. Each entry is:
 *     {
 *       id:      'unique-kebab-id',       // used as the key in output rows
 *       label:   'Human readable name',   // provenance for reports
 *       enabled: true | false,            // disabled sources are skipped
 *       fetch:   async (ctx) => ({ today, tomorrow })
 *     }
 *   ctx = { lat, lon, tz, today, tomorrow, fmt } where today/tomorrow are
 *   station-local 'YYYY-MM-DD' strings and fmt is an Intl.DateTimeFormat
 *   for the station timezone (use localDateOf(ms, fmt) to bucket UTC
 *   timestamps into station-local dates).
 *   fetch must return { today: {max, min} | null, tomorrow: {max, min} |
 *   null } in degrees Celsius, or throw on failure. A throw (or nulls)
 *   for one source never stops the run — nulls are recorded and the mode
 *   is computed from whatever sources answered.
 *   To add a source: write one fetch adapter, add one SOURCES entry.
 *   To remove one: set enabled:false (or delete the entry).
 *   When the user remembers his original websites, adapters for them can
 *   be dropped in here the same way.
 *
 * DEFAULT SOURCE SET (verified live from the build machine, 2026-11-14):
 *   1. wttr-in      wttr.in (World Weather Online data) — global.
 *   2. met-norway   MET Norway locationforecast 2.0 — global.
 *   3. 7timer       7timer.info civil product (NOAA GFS-based) — global.
 *   4. nws          NOAA National Weather Service api.weather.gov —
 *                   UNITED STATES ONLY (returns nulls elsewhere; that is
 *                   expected coverage behaviour, not an error).
 *
 *   ⚠️ SHORTFALL FLAG — read before trusting the source count:
 *   The intended fifth source, the Open-Meteo forecast API
 *   (api.open-meteo.com, best_match), could NOT be verified from the
 *   build machine on 2026-11-14: three separate live calls all returned
 *   {"error":true,"reason":"Daily API request limit exceeded"} — the
 *   free daily quota for this machine's shared egress IP was already
 *   exhausted. Its adapter is included below but DISABLED by default
 *   (enabled:false). Do not enable it until it has been verified with a
 *   successful live call from the machine that will run this script.
 *   With the default set, non-US cities therefore have 3 active sources
 *   and US cities 4 — the mode is computed from however many answered,
 *   and every row records exactly how many that was ("of").
 *
 * Output row format (ONE JSONL row per city per run, appended to
 * data/multisource/<UTC date of run>.jsonl):
 *   {
 *     "ts": "<run timestamp, ISO UTC, one value for the whole run>",
 *     "city": "<city slug from cities.json>",
 *     "station": "<station code from stations.json>",
 *     "lat": <station latitude>, "lon": <station longitude>,
 *     "tz": "<station IANA timezone (embedded map below)>",
 *     "days": [
 *       { "date": "<station-local today>",
 *         "values": { "<source id>": [max, min] | null, ... },
 *         "mode_max": { "value": <whole °C>, "agree": <n>, "of": <n> } | null,
 *         "mode_min": { "value": <whole °C>, "agree": <n>, "of": <n> } | null },
 *       { "date": "<station-local tomorrow>", ...same shape... }
 *     ],
 *     "source_errors": { "<source id>": "<short error>", ... }  // may be {}
 *   }
 *   Per-source values are °C rounded to 1dp. Mode values are whole °C:
 *   each source's value is rounded to the nearest whole degree, the most
 *   recurring value wins; ties are broken by the tied value closest to
 *   the mean of the rounded values, then by the lower value (deterministic,
 *   documented here so the 24h+ tests reproduce it exactly).
 *
 * Usage: node multisource.mjs
 * Exit code: 0 if at least one source value was collected across the run;
 *            1 if inputs are unreadable or literally nothing answered.
 */
import fs from 'fs';
import axios from 'axios';
import { fileURLToPath } from 'url';
import path from 'path';

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const CITIES_PATH = path.join(SCRIPT_DIR, 'cities.json');
const STATIONS_PATH = path.join(SCRIPT_DIR, 'stations.json');
const OUT_DIR = path.join(SCRIPT_DIR, 'data', 'multisource');

const CONCURRENCY = 4;
const UA = 'polyscalp-live multisource logger (+https://github.com/benniklaus878-glitch/polyscalp-live)';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const round1 = (v) => Math.round(v * 10) / 10;

const RUN_TS = new Date().toISOString();
const OUT_FILE = path.join(OUT_DIR, `${RUN_TS.slice(0, 10)}.jsonl`);

/**
 * Station IANA timezones, one per city in cities.json. Needed to bucket
 * the UTC-hourly sources (MET Norway, 7timer, NWS) into station-local
 * days and to know each station's local "today". Cross-audited at test
 * time against wttr.in's own location-local dates (see TEST_REPORT.md).
 */
const TZ_BY_CITY = {
  'amsterdam': 'Europe/Amsterdam',
  'ankara': 'Europe/Istanbul',
  'atlanta': 'America/New_York',
  'austin': 'America/Chicago',
  'beijing': 'Asia/Shanghai',
  'buenos-aires': 'America/Argentina/Buenos_Aires',
  'busan': 'Asia/Seoul',
  'cape-town': 'Africa/Johannesburg',
  'chengdu': 'Asia/Shanghai',
  'chicago': 'America/Chicago',
  'chongqing': 'Asia/Shanghai',
  'dallas': 'America/Chicago',
  'denver': 'America/Denver',
  'guangzhou': 'Asia/Shanghai',
  'helsinki': 'Europe/Helsinki',
  'hong-kong': 'Asia/Hong_Kong',
  'houston': 'America/Chicago',
  'istanbul': 'Europe/Istanbul',
  'jeddah': 'Asia/Riyadh',
  'jinan': 'Asia/Shanghai',
  'karachi': 'Asia/Karachi',
  'kuala-lumpur': 'Asia/Kuala_Lumpur',
  'london': 'Europe/London',
  'los-angeles': 'America/Los_Angeles',
  'lucknow': 'Asia/Kolkata',
  'madrid': 'Europe/Madrid',
  'manila': 'Asia/Manila',
  'mexico-city': 'America/Mexico_City',
  'miami': 'America/New_York',
  'milan': 'Europe/Rome',
  'moscow': 'Europe/Moscow',
  'munich': 'Europe/Berlin',
  'nyc': 'America/New_York',
  'panama-city': 'America/Panama',
  'paris': 'Europe/Paris',
  'qingdao': 'Asia/Shanghai',
  'san-francisco': 'America/Los_Angeles',
  'sao-paulo': 'America/Sao_Paulo',
  'seattle': 'America/Los_Angeles',
  'seoul': 'Asia/Seoul',
  'shanghai': 'Asia/Shanghai',
  'shenzhen': 'Asia/Shanghai',
  'singapore': 'Asia/Singapore',
  'taipei': 'Asia/Taipei',
  'tel-aviv': 'Asia/Jerusalem',
  'tokyo': 'Asia/Tokyo',
  'toronto': 'America/Toronto',
  'warsaw': 'Europe/Warsaw',
  'wellington': 'Pacific/Auckland',
  'wuhan': 'Asia/Shanghai',
  'zhengzhou': 'Asia/Shanghai',
};

// ---------------------------------------------------------------------
// Date helpers
// ---------------------------------------------------------------------
function makeFormatter(tz) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit',
  });
}

function localDateOf(ms, fmt) {
  const parts = fmt.formatToParts(new Date(ms));
  let y; let m; let d;
  for (const p of parts) {
    if (p.type === 'year') y = p.value;
    else if (p.type === 'month') m = p.value;
    else if (p.type === 'day') d = p.value;
  }
  return `${y}-${m}-${d}`;
}

function nextDateStr(dateStr) {
  const [y, m, d] = dateStr.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d) + 86400e3).toISOString().slice(0, 10);
}

// Reduce a list of {ms, temp} points to per-local-date {max, min}.
function bucketMaxMin(points, fmt) {
  const byDate = new Map();
  for (const { ms, temp } of points) {
    if (typeof temp !== 'number' || Number.isNaN(temp)) continue;
    const date = localDateOf(ms, fmt);
    const cur = byDate.get(date);
    if (!cur) byDate.set(date, { max: temp, min: temp });
    else {
      if (temp > cur.max) cur.max = temp;
      if (temp < cur.min) cur.min = temp;
    }
  }
  return byDate;
}

function pairFromBucket(byDate, date) {
  const b = byDate.get(date);
  if (!b) return null;
  return { max: round1(b.max), min: round1(b.min) };
}

// ---------------------------------------------------------------------
// Mode — the original bot's signal rule.
// Values are rounded to whole degrees (as the original compared values);
// the most recurring value wins. Tie-break: tied value closest to the
// mean of the rounded values, then the lower value.
// ---------------------------------------------------------------------
function modeOf(values) {
  if (!values || values.length === 0) return null;
  const rounded = values.map((v) => Math.round(v));
  const counts = new Map();
  for (const v of rounded) counts.set(v, (counts.get(v) || 0) + 1);
  const best = Math.max(...counts.values());
  const tied = [...counts.entries()].filter(([, c]) => c === best).map(([v]) => v);
  let value;
  if (tied.length === 1) {
    value = tied[0];
  } else {
    const mean = rounded.reduce((a, b) => a + b, 0) / rounded.length;
    tied.sort((a, b) => (Math.abs(a - mean) - Math.abs(b - mean)) || (a - b));
    value = tied[0];
  }
  return { value, agree: best, of: values.length };
}

// ---------------------------------------------------------------------
// HTTP helper with retries (per-source attempts/timeout are configurable
// in the SOURCES entries via `attempts` / `timeoutMs`).
// ---------------------------------------------------------------------
async function getJson(url, { params, headers, timeoutMs = 30000, attempts = 2 } = {}) {
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const res = await axios.get(url, { params, headers, timeout: timeoutMs });
      return res.data;
    } catch (e) {
      lastErr = e;
      if (i < attempts - 1) await sleep(1500 * (i + 1));
    }
  }
  throw lastErr;
}

// =====================================================================
// SOURCES — the plug-in registry (see header for how to extend it).
// =====================================================================
const SOURCES = [
  {
    id: 'wttr-in',
    label: 'wttr.in (World Weather Online)',
    enabled: true,
    // wttr.in's j1 JSON gives daily maxtempC/mintempC keyed by the
    // location's own local dates — matched against ctx.today/tomorrow.
    async fetch(ctx) {
      const data = await getJson(`https://wttr.in/${ctx.lat},${ctx.lon}`, {
        params: { format: 'j1' },
        headers: { 'User-Agent': UA },
        timeoutMs: 40000,
        attempts: 2,
      });
      if (!data || !Array.isArray(data.weather) || data.weather.length === 0) {
        throw new Error('bad response: missing weather array');
      }
      const byDate = new Map();
      for (const w of data.weather) {
        const max = parseFloat(w.maxtempC);
        const min = parseFloat(w.mintempC);
        if (w.date && !Number.isNaN(max) && !Number.isNaN(min)) {
          byDate.set(w.date, { max, min });
        }
      }
      const today = pairFromBucket(byDate, ctx.today);
      const tomorrow = pairFromBucket(byDate, ctx.tomorrow);
      if (!today && !tomorrow) {
        throw new Error('response dates do not cover station-local today/tomorrow');
      }
      return { today, tomorrow };
    },
  },
  {
    id: 'met-norway',
    label: 'MET Norway locationforecast 2.0 (yr.no data)',
    enabled: true,
    // Hourly (later 6-hourly) air_temperature series in UTC; bucketed
    // into station-local days with ctx.fmt. A descriptive User-Agent is
    // required by api.met.no's terms of service.
    async fetch(ctx) {
      const data = await getJson('https://api.met.no/weatherapi/locationforecast/2.0/compact', {
        params: { lat: ctx.lat, lon: ctx.lon },
        headers: { 'User-Agent': UA },
        timeoutMs: 30000,
        attempts: 2,
      });
      const series = data && data.properties && data.properties.timeseries;
      if (!Array.isArray(series) || series.length === 0) {
        throw new Error('bad response: missing timeseries');
      }
      const points = [];
      for (const e of series) {
        const t = e && e.data && e.data.instant && e.data.instant.details
          ? e.data.instant.details.air_temperature : undefined;
        const ms = Date.parse(e.time);
        if (typeof t === 'number' && !Number.isNaN(ms)) points.push({ ms, temp: t });
      }
      if (points.length === 0) throw new Error('bad response: no temperature points');
      const byDate = bucketMaxMin(points, ctx.fmt);
      return {
        today: pairFromBucket(byDate, ctx.today),
        tomorrow: pairFromBucket(byDate, ctx.tomorrow),
      };
    },
  },
  {
    id: '7timer',
    label: '7timer.info civil product (NOAA GFS-based)',
    enabled: true,
    // 3-hourly temp2m; `init` is the model run time (UTC) and each
    // dataseries entry's `timepoint` is hours after init.
    async fetch(ctx) {
      const data = await getJson('https://www.7timer.info/bin/api.pl', {
        params: { lon: ctx.lon, lat: ctx.lat, product: 'civil', output: 'json' },
        headers: { 'User-Agent': UA },
        timeoutMs: 30000,
        attempts: 3, // 7timer dropped TLS connections transiently in test
        // run 1 (12 cities, then recovered) — one extra attempt is cheap
        // insurance; see TEST_REPORT.md.
      });
      if (!data || typeof data.init !== 'string' || !Array.isArray(data.dataseries)) {
        throw new Error('bad response: missing init/dataseries');
      }
      const initStr = data.init; // YYYYMMDDHH, UTC
      const baseMs = Date.UTC(
        Number(initStr.slice(0, 4)), Number(initStr.slice(4, 6)) - 1,
        Number(initStr.slice(6, 8)), Number(initStr.slice(8, 10)),
      );
      if (Number.isNaN(baseMs)) throw new Error('bad response: unparseable init');
      const points = [];
      for (const e of data.dataseries) {
        if (typeof e.temp2m === 'number' && typeof e.timepoint === 'number') {
          points.push({ ms: baseMs + e.timepoint * 3600e3, temp: e.temp2m });
        }
      }
      if (points.length === 0) throw new Error('bad response: no temperature points');
      const byDate = bucketMaxMin(points, ctx.fmt);
      return {
        today: pairFromBucket(byDate, ctx.today),
        tomorrow: pairFromBucket(byDate, ctx.tomorrow),
      };
    },
  },
  {
    id: 'nws',
    label: 'NOAA National Weather Service (api.weather.gov) — US only',
    enabled: true,
    // Two-step: /points resolves the grid, then forecastHourly gives
    // hourly periods (startTime carries the local offset). Points outside
    // the US return a 404 problem document — that is coverage, not an
    // error, so it returns nulls quietly.
    async fetch(ctx) {
      const headers = { 'User-Agent': UA, Accept: 'application/geo+json' };
      const lat = Math.round(ctx.lat * 10000) / 10000;
      const lon = Math.round(ctx.lon * 10000) / 10000;
      let points;
      try {
        points = await getJson(`https://api.weather.gov/points/${lat},${lon}`, {
          headers, timeoutMs: 30000, attempts: 2,
        });
      } catch (e) {
        if (e && e.response && e.response.status === 404) {
          return { today: null, tomorrow: null }; // outside US coverage
        }
        throw e;
      }
      const hourlyUrl = points && points.properties && points.properties.forecastHourly;
      if (!hourlyUrl) throw new Error('bad response: no forecastHourly URL');
      const fc = await getJson(hourlyUrl, { headers, timeoutMs: 30000, attempts: 2 });
      const periods = fc && fc.properties && fc.properties.periods;
      if (!Array.isArray(periods) || periods.length === 0) {
        throw new Error('bad response: missing periods');
      }
      const pts = [];
      for (const p of periods) {
        const ms = Date.parse(p.startTime);
        let t = typeof p.temperature === 'number' ? p.temperature : null;
        if (t !== null && p.temperatureUnit === 'F') t = (t - 32) * 5 / 9;
        if (t !== null && !Number.isNaN(ms)) pts.push({ ms, temp: t });
      }
      if (pts.length === 0) throw new Error('bad response: no temperature periods');
      const byDate = bucketMaxMin(pts, ctx.fmt);
      return {
        today: pairFromBucket(byDate, ctx.today),
        tomorrow: pairFromBucket(byDate, ctx.tomorrow),
      };
    },
  },
  {
    id: 'open-meteo',
    label: 'Open-Meteo forecast API (best_match) — UNVERIFIED, see header flag',
    enabled: false, // ⚠️ Could not be verified 2026-11-14 (daily quota
    // exhausted on the build machine's egress IP). Verify with a live
    // call from the runtime machine before enabling.
    async fetch(ctx) {
      const data = await getJson('https://api.open-meteo.com/v1/forecast', {
        params: {
          latitude: ctx.lat,
          longitude: ctx.lon,
          daily: 'temperature_2m_max,temperature_2m_min',
          timezone: 'auto',
          forecast_days: 3,
        },
        headers: { 'User-Agent': UA },
        timeoutMs: 30000,
        attempts: 2,
      });
      if (!data || data.error) {
        throw new Error(`api error: ${data && data.reason ? data.reason : 'unknown'}`);
      }
      if (data.timezone && data.timezone !== ctx.tz) {
        log(`  note: open-meteo tz ${data.timezone} != station tz ${ctx.tz}`);
      }
      const daily = data.daily;
      if (!daily || !Array.isArray(daily.time)) throw new Error('bad response: missing daily');
      const pick = (date) => {
        const i = daily.time.indexOf(date);
        if (i < 0) return null;
        const max = daily.temperature_2m_max[i];
        const min = daily.temperature_2m_min[i];
        if (typeof max !== 'number' || typeof min !== 'number') return null;
        return { max: round1(max), min: round1(min) };
      };
      return { today: pick(ctx.today), tomorrow: pick(ctx.tomorrow) };
    },
  },
];

// =====================================================================
// Runner
// =====================================================================
async function processCity(city, stations, tally) {
  const info = stations[city];
  if (!info || !info.station || typeof info.lat !== 'number' || typeof info.lon !== 'number') {
    throw new Error(`station entry for "${city}" missing/incomplete in stations.json`);
  }
  const tz = TZ_BY_CITY[city];
  if (!tz) throw new Error(`city "${city}" missing from TZ_BY_CITY map`);
  const fmt = makeFormatter(tz);
  const today = localDateOf(Date.now(), fmt);
  const tomorrow = nextDateStr(today);
  const ctx = { lat: info.lat, lon: info.lon, tz, today, tomorrow, fmt };

  const active = SOURCES.filter((s) => s.enabled);
  const results = await Promise.all(active.map(async (src) => {
    try {
      const r = await src.fetch(ctx);
      return { id: src.id, ok: true, today: r.today || null, tomorrow: r.tomorrow || null };
    } catch (e) {
      const msg = (e && e.message ? e.message : String(e)).slice(0, 160);
      return { id: src.id, ok: false, error: msg, today: null, tomorrow: null };
    }
  }));

  const dayRow = (date, key) => {
    const values = {};
    const maxes = [];
    const mins = [];
    for (const r of results) {
      const v = r[key];
      values[r.id] = v ? [v.max, v.min] : null;
      if (v) {
        maxes.push(v.max);
        mins.push(v.min);
        tally.values += 1;
      }
      if (key === 'today') {
        // Tally each source once per city (on the today pass).
        if (r.ok && v) tally.perSource[r.id] = (tally.perSource[r.id] || 0) + 1;
        else if (r.ok) tally.perSourcePartial[r.id] = (tally.perSourcePartial[r.id] || 0) + 1;
      }
    }
    return { date, values, mode_max: modeOf(maxes), mode_min: modeOf(mins) };
  };

  const sourceErrors = {};
  for (const r of results) {
    if (!r.ok) sourceErrors[r.id] = r.error;
  }

  const row = {
    ts: RUN_TS,
    city,
    station: info.station,
    lat: info.lat,
    lon: info.lon,
    tz,
    days: [dayRow(today, 'today'), dayRow(tomorrow, 'tomorrow')],
    source_errors: sourceErrors,
  };
  fs.appendFileSync(OUT_FILE, JSON.stringify(row) + '\n');
  const failedIds = results.filter((r) => !r.ok).map((r) => r.id);
  log(`${city}: ok (${info.station}, ${tz}, ${today}/${tomorrow})`
    + (failedIds.length ? ` — sources failed: ${failedIds.join(', ')}` : ''));
  return row;
}

async function main() {
  const activeIds = SOURCES.filter((s) => s.enabled).map((s) => s.id);
  const disabledIds = SOURCES.filter((s) => !s.enabled).map((s) => s.id);
  log(`multisource logger starting (run ts=${RUN_TS})`);
  log(`active sources: ${activeIds.join(', ')}`);
  if (disabledIds.length) log(`disabled sources (not queried): ${disabledIds.join(', ')}`);

  let cities;
  let stations;
  try {
    cities = JSON.parse(fs.readFileSync(CITIES_PATH, 'utf8'));
  } catch (e) {
    log(`failed to read cities.json at ${CITIES_PATH}: ${e.message}`);
    process.exit(1);
  }
  try {
    stations = JSON.parse(fs.readFileSync(STATIONS_PATH, 'utf8'));
  } catch (e) {
    log(`failed to read stations.json at ${STATIONS_PATH}: ${e.message}`);
    process.exit(1);
  }
  if (!Array.isArray(cities)) {
    log('cities.json is not an array of slugs');
    process.exit(1);
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });

  const tally = { values: 0, perSource: {}, perSourcePartial: {} };
  let rows = 0;
  let cityFailures = 0;
  let idx = 0;
  const workers = Array.from({ length: Math.min(CONCURRENCY, cities.length) }, async () => {
    while (idx < cities.length) {
      const city = cities[idx++];
      try {
        await processCity(city, stations, tally);
        rows++;
      } catch (e) {
        cityFailures++;
        log(`${city}: CITY FAILED - ${e.message}`);
      }
    }
  });
  await Promise.all(workers);

  log(`summary: rows=${rows} city_failures=${cityFailures} source_day_values=${tally.values} output=${OUT_FILE}`);
  log(`per-source cities with a today value: ${JSON.stringify(tally.perSource)}`);
  if (Object.keys(tally.perSourcePartial).length) {
    log(`per-source cities ok-but-no-today-value (partial): ${JSON.stringify(tally.perSourcePartial)}`);
  }
  process.exit(tally.values > 0 ? 0 : 1);
}

await main();
