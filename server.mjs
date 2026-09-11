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

const HTML = `<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Matter telemetry</title>
<style>
body{font:16px system-ui,sans-serif;max-width:1120px;margin:28px auto;padding:0 18px;background:#111;color:#eee}h1{font-size:25px;margin-bottom:8px}h2{margin-top:32px}.status{color:#aaa;font-size:13px;margin-bottom:10px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:12px}.card{background:#1d1d1d;padding:14px;border-radius:9px}.v{font-size:28px;font-weight:650;margin-top:4px}.sub{color:#aaa;font-size:13px}.ok{color:#8fd18f}.bad{color:#ff9c9c}.charts{display:grid;grid-template-columns:1fr;gap:15px;margin-top:18px}canvas{width:100%;height:235px;background:#181818;border-radius:8px}.note{margin-top:12px;color:#aaa;font-size:13px}
</style></head><body>
<h1>Matter telemetry</h1>

<h2>GRILLPLATS power meter</h2><div id="pconn" class="status"></div>
<div class="cards">
<div class="card"><div>Current</div><div class="v" id="power">—</div><div class="sub" id="electrical"></div></div>
<div class="card"><div>Today</div><div class="v" id="today">—</div></div>
<div class="card"><div>Yesterday</div><div class="v" id="yesterday">—</div></div>
<div class="card"><div>Last 7 days</div><div class="v" id="seven">—</div></div>
<div class="card"><div>Last 30 days</div><div class="v" id="thirty">—</div></div>
</div>
<div class="charts"><canvas id="powerGraph" width="1040" height="235"></canvas></div>
<div class="note">* partial period: tracking began after the start of that period.</div>

<h2>ALPSTUGA air quality</h2><div id="aconn" class="status"></div>
<div class="cards">
<div class="card"><div>Air quality</div><div class="v" id="aq">—</div></div>
<div class="card"><div>CO₂</div><div class="v" id="co2">—</div><div class="sub">ppm</div></div>
<div class="card"><div>PM2.5</div><div class="v" id="pm25">—</div><div class="sub">µg/m³</div></div>
<div class="card"><div>Temperature</div><div class="v" id="temp">—</div></div>
<div class="card"><div>Relative humidity</div><div class="v" id="rh">—</div></div>
</div>
<div class="charts">
<canvas id="co2Graph" width="1040" height="235"></canvas>
<canvas id="pmGraph" width="1040" height="235"></canvas>
<canvas id="tempGraph" width="1040" height="235"></canvas>
<canvas id="rhGraph" width="1040" height="235"></canvas>
</div>
<script>
const fmtEnergy=e=>e==null?'—':e.toFixed(3)+' kWh';
function setPeriod(id,p){document.getElementById(id).textContent=fmtEnergy(p?.kwh)+(p?.partial?' *':'')}
function setText(id,v){document.getElementById(id).textContent=v}
function statusLine(id,connected,lastSeen,label){const e=document.getElementById(id);e.className='status '+(connected?'ok':'bad');e.textContent=(connected?'Matter connected':'Matter disconnected')+(lastSeen?' · '+label+' '+new Date(lastSeen).toLocaleTimeString():'')}
function draw(cId,a,key,label,unit,minFloor=null){
 const c=document.getElementById(cId),x=c.getContext('2d');x.clearRect(0,0,c.width,c.height);if(a.length<2){x.fillStyle='#aaa';x.fillText(label+' · waiting for history',12,18);return}
 const pts=a.filter(p=>p[key]!=null);if(pts.length<2)return;
 const vals=pts.map(p=>Number(p[key]));let min=Math.min(...vals),max=Math.max(...vals);if(minFloor!==null)min=Math.min(min,minFloor);if(max===min){max+=1;min-=1}
 const pad=(max-min)*0.08;min-=pad;max+=pad;const now=Date.now(),start=now-86400000;
 x.strokeStyle='#888';x.beginPath();for(let i=0;i<pts.length;i++){const px=10+Math.max(0,Math.min(1,(pts[i].ts_ms-start)/(now-start)))*(c.width-20),py=c.height-10-((vals[i]-min)/(max-min))*(c.height-24);if(i===0)x.moveTo(px,py);else x.lineTo(px,py)}x.stroke();x.fillStyle='#aaa';x.fillText(label+' · 24 h · '+vals[vals.length-1].toFixed(key==='temperature_c'?1:0)+' '+unit,12,16)
}
async function refresh(){
 const [p,a,ph,ah]=await Promise.all([
  fetch('/api/power-status').then(r=>r.json()),fetch('/api/air-status').then(r=>r.json()),
  fetch('/api/power-history?hours=24').then(r=>r.json()),fetch('/api/air-history?hours=24').then(r=>r.json())]);
 setText('power',p.powerW==null?'—':p.powerW.toFixed(1)+' W');
 setText('electrical',(p.voltageV==null?'':p.voltageV.toFixed(0)+' V')+(p.currentA==null?'':' · '+(p.currentA*1000).toFixed(0)+' mA'));
 setPeriod('today',p.today);setPeriod('yesterday',p.yesterday);setPeriod('seven',p.sevenDays);setPeriod('thirty',p.thirtyDays);statusLine('pconn',p.connected,p.lastSeenMs,'last data');
 setText('aq',a.airQualityText??'—');setText('co2',a.co2Ppm==null?'—':a.co2Ppm.toFixed(0));setText('pm25',a.pm25Ugm3==null?'—':a.pm25Ugm3.toFixed(1));setText('temp',a.temperatureC==null?'—':a.temperatureC.toFixed(2)+' °C');setText('rh',a.humidityPct==null?'—':a.humidityPct.toFixed(2)+' %');statusLine('aconn',a.connected,a.lastSeenMs,'last data');
 draw('powerGraph',ph,'power_w','Power','W',0);draw('co2Graph',ah,'co2_ppm','CO₂','ppm');draw('pmGraph',ah,'pm25_ugm3','PM2.5','µg/m³',0);draw('tempGraph',ah,'temperature_c','Temperature','°C');draw('rhGraph',ah,'humidity_pct','Relative humidity','%',0);
}
refresh();setInterval(refresh,15000);
</script></body></html>`;

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
