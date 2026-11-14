/**
 * ensemble.mjs — PolyScalp model-trader rig, piece 3 (per MODEL_TRADER_SCOPE.md)
 *
 * Purpose:
 *   Log ECMWF IFS ensemble temperature forecasts for every PolyScalp city,
 *   at the exact station each city's Polymarket markets resolve on, so the
 *   model-vs-market accuracy of the ensemble can be tested after >=4 weeks
 *   of clean data (alongside the order-book / METAR logger). This script
 *   does NOT trade, place orders, or touch any wallet — it only logs.
 *
 * API (verified live 2026-11-14):
 *   GET https://ensemble-api.open-meteo.com/v1/ensemble
 *   params: latitude, longitude, hourly=temperature_2m,
 *           models=ecmwf_ifs025, forecast_days=3, timezone=auto
 *   NOTE: models=ecmwf_ifs (without 025) returns ONLY the mean.
 *         The script must use ecmwf_ifs025.
 *   Response JSON: {
 *     latitude, longitude,          // model's snapped grid point
 *     timezone,                     // IANA string, station-local
 *     utc_offset_seconds,
 *     hourly: {
 *       time: ["YYYY-MM-DDTHH:MM", ...],   // station-local wall time
 *       temperature_2m: [...],             // control member
 *       temperature_2m_member01: [...],    // ensemble member 01
 *       ...
 *       temperature_2m_member50: [...]     // ensemble member 50
 *     }
 *   }
 *   Total 51 members: control (temperature_2m) + member01..member50.
 *   Members are DISCOVERED from the response keys matching
 *   /^temperature_2m(_member\d+)?$/ rather than assumed, and the
 *   discovered count is logged (asserted in the log output) per city.
 *
 * Output row format (ONE JSONL row per city, appended to
 * data/ensemble/<UTC date of run>.jsonl):
 *   {
 *     "ts": "<run timestamp, ISO UTC, one value for the whole run>",
 *     "city": "<city slug from cities.json>",
 *     "station": "<station code from stations.json>",
 *     "lat": <station latitude from stations.json>,
 *     "lon": <station longitude from stations.json>,
 *     "grid_lat": <response latitude, snapped grid point>,
 *     "grid_lon": <response longitude, snapped grid point>,
 *     "tz": "<response timezone string>",
 *     "utc_offset_seconds": <response utc_offset_seconds>,
 *     "model": "ecmwf_ifs025",
 *     "members": <count discovered, expected 51>,
 *     "days": [
 *       { "date": "<station-local today, YYYY-MM-DD>",
 *         "pairs": [[max, min], ...] },   // one [max, min] per member,
 *                                         // control first, then member01..50;
 *                                         // max/min in °C, rounded to 2dp;
 *                                         // [null, null] if a member has
 *                                         // no values that day
 *       { "date": "<station-local tomorrow, YYYY-MM-DD>",
 *         "pairs": [[max, min], ...] }
 *     ]
 *   }
 *
 * Usage:
 *   node ensemble.mjs
 *
 * Exit code: 0 if at least one city succeeded; 1 only if ALL cities failed.
 * A failed city is logged with its reason and does not stop the run.
 */
import fs from 'fs';
import axios from 'axios';
import { fileURLToPath } from 'url';
import path from 'path';

const API_URL = 'https://ensemble-api.open-meteo.com/v1/ensemble';
const MODEL = 'ecmwf_ifs025';
const CONCURRENCY = 6;
const MAX_ATTEMPTS = 3;
const TIMEOUT_MS = 30000;

// Resolve inputs and output relative to this script's own directory.
// In the target repo the script sits at repo root next to cities.json,
// so these paths coincide with the repo root.
const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const CITIES_PATH = path.join(SCRIPT_DIR, 'cities.json');
const STATIONS_PATH = path.join(SCRIPT_DIR, 'stations.json');
const OUT_DIR = path.join(SCRIPT_DIR, 'data', 'ensemble');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const round2 = (v) => Math.round(v * 100) / 100;

// One run timestamp (ISO UTC) for the whole run, and the UTC date of the run
// determines the output file name.
const RUN_TS = new Date().toISOString();
const OUT_FILE = path.join(OUT_DIR, `${RUN_TS.slice(0, 10)}.jsonl`);

async function fetchEnsemble(lat, lon) {
  let lastErr = null;
  for (let i = 0; i < MAX_ATTEMPTS; i++) {
    try {
      const res = await axios.get(API_URL, {
        timeout: TIMEOUT_MS,
        params: {
          latitude: lat,
          longitude: lon,
          hourly: 'temperature_2m',
          models: MODEL,
          forecast_days: 3,
          timezone: 'auto',
        },
      });
      return res.data;
    } catch (e) {
      lastErr = e;
      if (i < MAX_ATTEMPTS - 1) {
        await sleep(1500 * (i + 1));
      }
    }
  }
  throw lastErr;
}

function discoverMembers(hourly) {
  const keys = Object.keys(hourly).filter((k) => /^temperature_2m(_member\d+)?$/.test(k));
  keys.sort((a, b) => {
    if (a === 'temperature_2m') return -1;
    if (b === 'temperature_2m') return 1;
    const na = parseInt(a.slice('temperature_2m_member'.length), 10);
    const nb = parseInt(b.slice('temperature_2m_member'.length), 10);
    return na - nb;
  });
  return keys;
}

function pairsForDate(hourly, memberKeys, times, targetDate) {
  // Indices belonging to the target local date (date prefix of the
  // station-local time strings).
  const idxs = [];
  for (let i = 0; i < times.length; i++) {
    if (typeof times[i] === 'string' && times[i].slice(0, 10) === targetDate) {
      idxs.push(i);
    }
  }
  return memberKeys.map((key) => {
    const series = hourly[key];
    let max = null;
    let min = null;
    for (const i of idxs) {
      const v = Array.isArray(series) ? series[i] : null;
      if (v === null || v === undefined || typeof v !== 'number' || Number.isNaN(v)) continue;
      if (max === null || v > max) max = v;
      if (min === null || v < min) min = v;
    }
    if (max === null || min === null) return [null, null];
    return [round2(max), round2(min)];
  });
}

async function processCity(city, stations) {
  const info = stations[city];
  if (!info) {
    throw new Error(`city "${city}" missing from stations.json`);
  }
  if (!info.station || typeof info.lat !== 'number' || typeof info.lon !== 'number') {
    throw new Error(`station entry for "${city}" is incomplete in stations.json`);
  }

  const data = await fetchEnsemble(info.lat, info.lon);

  if (!data || typeof data !== 'object') {
    throw new Error('bad response: not an object');
  }
  if (!data.hourly || typeof data.hourly !== 'object') {
    throw new Error('bad response: missing hourly');
  }
  if (!Array.isArray(data.hourly.time)) {
    throw new Error('bad response: missing hourly.time');
  }
  if (typeof data.utc_offset_seconds !== 'number') {
    throw new Error('bad response: missing utc_offset_seconds');
  }
  if (typeof data.latitude !== 'number' || typeof data.longitude !== 'number') {
    throw new Error('bad response: missing grid latitude/longitude');
  }
  if (!data.timezone) {
    throw new Error('bad response: missing timezone');
  }

  const memberKeys = discoverMembers(data.hourly);
  if (memberKeys.length === 0) {
    throw new Error('bad response: no temperature_2m member keys found');
  }
  // Assert the discovered member count in the log output.
  log(`${city}: members discovered: ${memberKeys.length} (expected 51)`);
  if (memberKeys.length !== 51) {
    log(`${city}: WARNING members count is ${memberKeys.length}, expected 51`);
  }

  // Station-local today from the response's utc_offset_seconds.
  const offset = data.utc_offset_seconds;
  const localNowMs = Date.now() + offset * 1000;
  const localNow = new Date(localNowMs);
  const today = localNow.toISOString().slice(0, 10);
  const tomorrow = new Date(localNowMs + 86400e3).toISOString().slice(0, 10);

  const times = data.hourly.time;
  const days = [
    { date: today, pairs: pairsForDate(data.hourly, memberKeys, times, today) },
    { date: tomorrow, pairs: pairsForDate(data.hourly, memberKeys, times, tomorrow) },
  ];

  const row = {
    ts: RUN_TS,
    city,
    station: info.station,
    lat: info.lat,
    lon: info.lon,
    grid_lat: data.latitude,
    grid_lon: data.longitude,
    tz: data.timezone,
    utc_offset_seconds: data.utc_offset_seconds,
    model: MODEL,
    members: memberKeys.length,
    days,
  };

  fs.appendFileSync(OUT_FILE, JSON.stringify(row) + '\n');
  return row;
}

async function main() {
  log(`ensemble logger starting (model=${MODEL}, run ts=${RUN_TS})`);

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

  let ok = 0;
  let failed = 0;
  let idx = 0;

  // Small concurrency pool: max ~6 in flight.
  const workers = Array.from({ length: Math.min(CONCURRENCY, cities.length) }, async () => {
    while (idx < cities.length) {
      const city = cities[idx++];
      try {
        await processCity(city, stations);
        ok++;
        log(`${city}: ok`);
      } catch (e) {
        failed++;
        log(`${city}: FAILED - ${e.message}`);
      }
    }
  });
  await Promise.all(workers);

  log(`summary: cities ok=${ok} failed=${failed} output=${OUT_FILE}`);
  process.exit(ok > 0 ? 0 : 1);
}

await main();
