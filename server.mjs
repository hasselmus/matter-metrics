import http from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';

const POWER_NODE_ID = Number(process.env.POWER_NODE_ID ?? process.env.MATTER_NODE_ID ?? 1);
const AIR_NODE_ID = Number(process.env.AIR_NODE_ID ?? 2);
const WS_URL = process.env.MATTER_WS_URL ?? 'ws://127.0.0.1:5580/ws';
const PORT = Number(process.env.PORT ?? 8791);
const DB_PATH = process.env.DB_PATH ?? '/var/lib/matter-metrics/metrics.sqlite';

fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
const db = new DatabaseSync(DB_PATH);
db.exec(`
  PRAGMA journal_mode=WAL;
  PRAGMA synchronous=NORMAL;
  CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  ) STRICT;
  CREATE TABLE IF NOT EXISTS power_samples (
    ts_ms INTEGER PRIMARY KEY,
    power_mw INTEGER,
    voltage_mv INTEGER,
    current_ma INTEGER
  ) STRICT;
  CREATE TABLE IF NOT EXISTS energy_samples (
    ts_ms INTEGER PRIMARY KEY,
    raw_mwh INTEGER NOT NULL,
    end_systime_ms INTEGER,
    virtual_mwh INTEGER NOT NULL
  ) STRICT;
  CREATE INDEX IF NOT EXISTS idx_energy_ts ON energy_samples(ts_ms);
  CREATE TABLE IF NOT EXISTS air_samples (
    ts_ms INTEGER PRIMARY KEY,
    temperature_c REAL,
    humidity_pct REAL,
    co2_ppm REAL,
    pm25_ugm3 REAL,
    air_quality INTEGER
  ) STRICT;
  CREATE INDEX IF NOT EXISTS idx_air_ts ON air_samples(ts_ms);
`);

const kvGetStmt = db.prepare('SELECT value FROM kv WHERE key=?');
const kvSetStmt = db.prepare('INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value');
const powerPutStmt = db.prepare('INSERT INTO power_samples(ts_ms,power_mw,voltage_mv,current_ma) VALUES(?,?,?,?) ON CONFLICT(ts_ms) DO UPDATE SET power_mw=excluded.power_mw, voltage_mv=excluded.voltage_mv, current_ma=excluded.current_ma');
const energyPutStmt = db.prepare('INSERT OR REPLACE INTO energy_samples(ts_ms,raw_mwh,end_systime_ms,virtual_mwh) VALUES(?,?,?,?)');
const airPutStmt = db.prepare('INSERT INTO air_samples(ts_ms,temperature_c,humidity_pct,co2_ppm,pm25_ugm3,air_quality) VALUES(?,?,?,?,?,?) ON CONFLICT(ts_ms) DO UPDATE SET temperature_c=excluded.temperature_c, humidity_pct=excluded.humidity_pct, co2_ppm=excluded.co2_ppm, pm25_ugm3=excluded.pm25_ugm3, air_quality=excluded.air_quality');

function kvGet(key) { return kvGetStmt.get(key)?.value ?? null; }
function kvSet(key, value) { kvSetStmt.run(key, String(value)); }
function numOrNull(v) { return v === null || v === undefined || v === '' ? null : Number(v); }
function finiteOrNull(v) { const n = numOrNull(v); return Number.isFinite(n) ? n : null; }
function minuteBucket(ms = Date.now()) { return Math.floor(ms / 60000) * 60000; }

let powerState = {
  connected: false,
  powerMw: null,
  voltageMv: null,
  currentMa: null,
  rawMwh: finiteOrNull(kvGet('last_raw_mwh')),
  endSystimeMs: finiteOrNull(kvGet('last_end_systime_ms')),
  energyOffsetMwh: Number(kvGet('energy_offset_mwh') ?? 0),
  virtualMwh: finiteOrNull(kvGet('last_virtual_mwh')),
  lastSeenMs: finiteOrNull(kvGet('power_last_seen_ms') ?? kvGet('last_seen_ms')),
};

let airState = {
  connected: false,
  temperatureC: null,
  humidityPct: null,
  co2Ppm: null,
  pm25Ugm3: null,
  airQuality: null,
  lastSeenMs: finiteOrNull(kvGet('air_last_seen_ms')),
};

function samplePower() {
  if (powerState.powerMw === null && powerState.voltageMv === null && powerState.currentMa === null) return;
  powerPutStmt.run(minuteBucket(), powerState.powerMw, powerState.voltageMv, powerState.currentMa);
}
function sampleAir() {
  if ([airState.temperatureC, airState.humidityPct, airState.co2Ppm, airState.pm25Ugm3, airState.airQuality].every(v => v === null)) return;
  airPutStmt.run(minuteBucket(), airState.temperatureC, airState.humidityPct, airState.co2Ppm, airState.pm25Ugm3, airState.airQuality);
}

function processEnergy(value, ts = Date.now()) {
  if (!value || typeof value !== 'object') return;
  const raw = Number(value['0']);
  if (!Number.isFinite(raw)) return;
  const sys = value['4'] === undefined || value['4'] === null ? null : Number(value['4']);

  if (powerState.rawMwh !== null) {
    const rawWentBackwards = raw < powerState.rawMwh;
    const systimeWentBackwards = sys !== null && powerState.endSystimeMs !== null && sys < powerState.endSystimeMs;
    if (rawWentBackwards || systimeWentBackwards) {
      powerState.energyOffsetMwh += powerState.rawMwh;
      kvSet('energy_offset_mwh', powerState.energyOffsetMwh);
      console.log(`Energy counter reset detected: raw ${powerState.rawMwh} -> ${raw}, systime ${powerState.endSystimeMs} -> ${sys}; offset=${powerState.energyOffsetMwh} mWh`);
    }
  }

  powerState.rawMwh = raw;
  powerState.endSystimeMs = Number.isFinite(sys) ? sys : null;
  powerState.virtualMwh = powerState.energyOffsetMwh + raw;
  powerState.lastSeenMs = ts;
  kvSet('last_raw_mwh', raw);
  if (powerState.endSystimeMs !== null) kvSet('last_end_systime_ms', powerState.endSystimeMs);
  kvSet('last_virtual_mwh', powerState.virtualMwh);
  kvSet('power_last_seen_ms', ts);
  energyPutStmt.run(ts, raw, powerState.endSystimeMs, powerState.virtualMwh);
}

const POWER_PATHS = new Set(['2/144/4', '2/144/5', '2/144/8', '2/145/1']);
const AIR_PATHS = new Set(['1/91/0', '1/1026/0', '1/1029/0', '1/1037/0', '1/1066/0']);

function processPowerAttribute(p, value, ts = Date.now()) {
  if (!POWER_PATHS.has(p)) return;
  if (p === '2/144/4') powerState.voltageMv = finiteOrNull(value);
  if (p === '2/144/5') powerState.currentMa = finiteOrNull(value);
  if (p === '2/144/8') powerState.powerMw = finiteOrNull(value);
  if (p === '2/145/1') processEnergy(value, ts);
  powerState.lastSeenMs = ts;
  kvSet('power_last_seen_ms', ts);
  if (p !== '2/145/1') samplePower();
}

function processAirAttribute(p, value, ts = Date.now()) {
  if (!AIR_PATHS.has(p)) return;
  const n = finiteOrNull(value);
  if (p === '1/91/0') airState.airQuality = n;
  if (p === '1/1026/0') airState.temperatureC = n === null ? null : n / 100;
  if (p === '1/1029/0') airState.humidityPct = n === null ? null : n / 100;
  if (p === '1/1037/0') airState.co2Ppm = n;
  if (p === '1/1066/0') airState.pm25Ugm3 = n;
  airState.lastSeenMs = ts;
  kvSet('air_last_seen_ms', ts);
  sampleAir();
}

let ws;
let reconnectTimer;
function send(message_id, command, args) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const m = { message_id, command };
  if (args !== undefined) m.args = args;
  ws.send(JSON.stringify(m));
}
function connectMatter() {
  clearTimeout(reconnectTimer);
  console.log(`Connecting to ${WS_URL}`);
  ws = new WebSocket(WS_URL);
  ws.addEventListener('open', () => {
    powerState.connected = true;
    airState.connected = true;
    console.log('Matter WebSocket connected');
    send('listen', 'start_listening');
    setTimeout(() => {
      send('power-read', 'read_attribute', { node_id: POWER_NODE_ID, attribute_path: [...POWER_PATHS] });
      send('air-read', 'read_attribute', { node_id: AIR_NODE_ID, attribute_path: [...AIR_PATHS] });
    }, 500);
  });
  ws.addEventListener('message', ev => {
    let m;
    try { m = JSON.parse(ev.data); } catch { return; }
    if (m.message_id === 'power-read' && m.result) {
      const ts = Date.now();
      for (const [p, value] of Object.entries(m.result)) processPowerAttribute(p, value, ts);
      samplePower();
    }
    if (m.message_id === 'air-read' && m.result) {
      const ts = Date.now();
      for (const [p, value] of Object.entries(m.result)) processAirAttribute(p, value, ts);
      sampleAir();
    }
    if (m.event === 'attribute_updated' && Array.isArray(m.data)) {
      const [node, p, value] = m.data;
      if (Number(node) === POWER_NODE_ID) processPowerAttribute(p, value);
      if (Number(node) === AIR_NODE_ID) processAirAttribute(p, value);
    }
  });
  ws.addEventListener('close', () => {
    powerState.connected = false;
    airState.connected = false;
    console.log('Matter WebSocket disconnected; retrying in 5s');
    reconnectTimer = setTimeout(connectMatter, 5000);
  });
  ws.addEventListener('error', e => console.error('Matter WebSocket error:', e.message ?? e));
}
connectMatter();
setInterval(() => { samplePower(); sampleAir(); }, 60000).unref();

function localMidnight(daysBack = 0) {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - daysBack);
  return d.getTime();
}
function readingAtOrBefore(ts) {
  return db.prepare('SELECT virtual_mwh, ts_ms FROM energy_samples WHERE ts_ms <= ? ORDER BY ts_ms DESC LIMIT 1').get(ts) ?? null;
}
function firstReadingAtOrAfter(ts) {
  return db.prepare('SELECT virtual_mwh, ts_ms FROM energy_samples WHERE ts_ms >= ? ORDER BY ts_ms ASC LIMIT 1').get(ts) ?? null;
}
function latestReading() {
  return db.prepare('SELECT virtual_mwh, ts_ms FROM energy_samples ORDER BY ts_ms DESC LIMIT 1').get() ?? null;
}
function periodEnergy(start, end = Date.now()) {
  let a = readingAtOrBefore(start);
  let partial = false;
  if (!a) { a = firstReadingAtOrAfter(start); partial = true; }
  const b = readingAtOrBefore(end);
  if (!a || !b || b.ts_ms < a.ts_ms) return { kwh: null, partial: true };
  return { kwh: (b.virtual_mwh - a.virtual_mwh) / 1_000_000, partial };
}
function powerStatus() {
  const now = Date.now();
  return {
    nodeId: POWER_NODE_ID,
    connected: powerState.connected,
    powerW: powerState.powerMw === null ? null : powerState.powerMw / 1000,
    voltageV: powerState.voltageMv === null ? null : powerState.voltageMv / 1000,
    currentA: powerState.currentMa === null ? null : powerState.currentMa / 1000,
    today: periodEnergy(localMidnight(0), now),
    yesterday: periodEnergy(localMidnight(1), localMidnight(0)),
    sevenDays: periodEnergy(localMidnight(7), now),
    thirtyDays: periodEnergy(localMidnight(30), now),
    lastSeenMs: powerState.lastSeenMs,
  };
}
const AIR_QUALITY = ['Unknown', 'Good', 'Fair', 'Moderate', 'Poor', 'Very poor', 'Extremely poor'];
function airStatus() {
  return {
    nodeId: AIR_NODE_ID,
    connected: airState.connected,
    temperatureC: airState.temperatureC,
    humidityPct: airState.humidityPct,
    co2Ppm: airState.co2Ppm,
    pm25Ugm3: airState.pm25Ugm3,
    airQuality: airState.airQuality,
    airQualityText: airState.airQuality === null ? null : (AIR_QUALITY[airState.airQuality] ?? `Code ${airState.airQuality}`),
    lastSeenMs: airState.lastSeenMs,
  };
}

function json(res, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(body);
}

const HTML = fs.readFileSync(new URL('./public/index.html', import.meta.url), 'utf8');

const server = http.createServer((req, res) => {
  const u = new URL(req.url, `http://${req.headers.host ?? 'localhost'}`);
  if (u.pathname === '/') { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return res.end(HTML); }
  if (u.pathname === '/api/power-status' || u.pathname === '/api/status') return json(res, powerStatus());
  if (u.pathname === '/api/air-status') return json(res, airStatus());
  if (u.pathname === '/api/power-history' || u.pathname === '/api/history') {
    const hours = Math.min(168, Math.max(1, Number(u.searchParams.get('hours') ?? 24)));
    const since = Date.now() - hours * 3600000;
    const rows = db.prepare('SELECT ts_ms,power_mw/1000.0 AS power_w,voltage_mv/1000.0 AS voltage_v,current_ma/1000.0 AS current_a FROM power_samples WHERE ts_ms >= ? ORDER BY ts_ms').all(since);
    return json(res, rows);
  }
  if (u.pathname === '/api/air-history') {
    const hours = Math.min(720, Math.max(1, Number(u.searchParams.get('hours') ?? 24)));
    const since = Date.now() - hours * 3600000;
    const rows = db.prepare('SELECT ts_ms,temperature_c,humidity_pct,co2_ppm,pm25_ugm3,air_quality FROM air_samples WHERE ts_ms >= ? ORDER BY ts_ms').all(since);
    return json(res, rows);
  }
  res.writeHead(404); res.end('Not found');
});
server.listen(PORT, '0.0.0.0', () => console.log(`matter-metrics web UI: http://0.0.0.0:${PORT}`));
