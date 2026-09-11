# matter-metrics

Small standalone Matter telemetry collector and web UI for a Raspberry Pi.

It talks to an existing **Open Home Foundation Matter Server** over its local WebSocket API, stores selected telemetry in SQLite, and serves a simple browser dashboard. Home Assistant is not required.

The current device mappings cover:

- **IKEA GRILLPLATS** smart plug: active power, voltage, current and cumulative imported energy.
- **IKEA ALPSTUGA** air-quality sensor: air-quality state, temperature, relative humidity, CO₂ and PM2.5.

The intended topology is:

```text
Matter-over-Thread devices
          │
   Thread border router
     (e.g. Apple TV)
          │ IPv6/LAN
          ▼
   matter-server :5580
          │ WebSocket
          ▼
    matter-metrics
       ├─ SQLite
       └─ HTTP :8791
```

The Matter devices can remain commissioned into Apple Home at the same time; the Pi is simply another Matter administrator/fabric.

## Requirements

- 64-bit Linux on Raspberry Pi or similar
- Node.js 22.13 or newer
- Open Home Foundation `matter-server` already commissioned to the devices
- Working IPv6/mDNS connectivity to the Thread border router

`server.mjs` itself has no third-party npm dependencies; it uses Node's built-in HTTP, WebSocket and SQLite facilities.

## Defaults

```text
Matter Server WebSocket   ws://127.0.0.1:5580/ws
GRILLPLATS node            1
ALPSTUGA node              2
SQLite database            /var/lib/matter-metrics/metrics.sqlite
Web UI                     0.0.0.0:8791
```

Override these with environment variables:

```text
MATTER_WS_URL
POWER_NODE_ID
AIR_NODE_ID
DB_PATH
PORT
```

Example:

```bash
sudo install -d -o pi -g pi /opt/matter-metrics /var/lib/matter-metrics
sudo install -o pi -g pi -m 0644 server.mjs /opt/matter-metrics/server.mjs
node /opt/matter-metrics/server.mjs
```

Then open `http://<pi-address>:8791/`.

## systemd

Example units are under `systemd/`. They assume:

- user/group `pi`
- matter-server installed under `/opt/matter-server/node_modules/matter-server`
- Ethernet interface `eth0`
- Europe/Stockholm timezone

Adjust those values for another installation.

After copying the units:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now matter-server matter-metrics
```

Logs:

```bash
journalctl -u matter-server -f
journalctl -u matter-metrics -f
```

## GRILLPLATS energy accounting

GRILLPLATS exposes standard Matter Electrical Power Measurement and Electrical Energy Measurement clusters. The hardware cumulative-energy counter resets after a loss of mains power, so matter-metrics maintains a virtual cumulative counter across detected device resets. Daily/7-day/30-day energy values are calculated from that virtual counter rather than integrating instantaneous watts.

## ALPSTUGA mappings

The current ALPSTUGA mapping uses standard Matter attributes on endpoint 1:

```text
1/91/0       AirQuality
1/1026/0     TemperatureMeasurement.MeasuredValue
1/1029/0     RelativeHumidityMeasurement.MeasuredValue
1/1037/0     CarbonDioxideConcentrationMeasurement.MeasuredValue
1/1066/0     Pm25ConcentrationMeasurement.MeasuredValue
```

## Do not commit runtime state

There are no credentials or secrets in the source code or example service files. **Runtime state is different:**

- `/var/lib/matter-server` contains Matter fabric/controller state and cryptographic material. Treat it as secret.
- `metrics.sqlite` contains household telemetry/history and should normally remain local.
- `.env` or local configuration files may contain installation-specific information.

The repository `.gitignore` excludes these common runtime files.

## Status

This is deliberately a small personal telemetry service rather than a general Matter framework. Device mappings can be extended as additional Matter devices are added.