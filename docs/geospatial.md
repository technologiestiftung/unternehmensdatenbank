# Self-hosting Overpass API for Berlin

This repo includes optional Docker Compose services for a local Overpass instance loaded with the Berlin OSM extract, plus an optional Nominatim instance for reverse geocoding.

Once Overpass is running, point your pipeline at:

```bash
OVERPASS_URL=http://localhost:12345/api/interpreter
```

No repo code changes are required for that integration.

## Requirements

- Docker + Docker Compose
- about 4 GB disk
- about 2 GB RAM during Overpass import
- about 512 MB RAM for Overpass runtime

## Compose profiles

This repo keeps geospatial services out of the default startup flow.

- `geo`: starts Overpass
- `geocoding`: starts Nominatim

That means `./scripts/dev-up.sh` stays fast and does not trigger a large OSM import.

## Overpass service

The service is defined in [docker-compose.yml](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/docker-compose.yml:1) with:

- image `wiktorn/overpass-api:latest`
- Berlin Geofabrik extract
- host port `12345` by default
- persistent volume `overpass-db`

Important env vars in [.env](/Users/jonathanhaudenschild/Documents/Work/unternehmensdatenbank/.env:1):

- `OVERPASS_PORT=12345`
- `OVERPASS_MODE=init`
- `OVERPASS_PLANET_URL=https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf`
- `OVERPASS_DIFF_URL=https://download.geofabrik.de/europe/germany/berlin-updates/`
- `OVERPASS_SPACE=2000000000`

### First start

```bash
docker compose --profile geo up -d overpass
docker compose logs -f overpass
```

The initial import downloads and loads the Berlin extract. Expect roughly 5 to 15 minutes depending on network and local Docker resources.

Import is done when the logs show:

```text
Dispatcher started.
```

### After first import

Change `OVERPASS_MODE` from `init` to `clone` in `.env` if you want hourly diffs from Geofabrik on restarts.

If you leave it at `init`, the service remains configured for initial import mode.

### Verify Overpass

```bash
curl http://localhost:12345/api/status
```

```bash
curl -s "http://localhost:12345/api/interpreter" \
  --data '[out:json][timeout:10];node(around:100,52.5219,13.4132)[name];out body;' \
  | python3 -m json.tool | head -40
```

### Optional manual update

```bash
docker exec overpass-berlin /bin/bash -c \
  "cd /db && fetch_osc.sh 1 https://download.geofabrik.de/europe/germany/berlin-updates/ osmChange && apply_osc_to_db.sh 1 osmChange /db/rules/"
```

Note: this command assumes the container name `overpass-berlin`. This repo currently relies on the Compose service name `overpass`, so prefer changing the service env to `clone` for ongoing updates unless you also choose to add a fixed `container_name`.

## Nominatim

Nominatim is included as an optional separate profile because it is heavier and imports more slowly.

This repo uses host port `8081` by default, to stay clear of the 8080 range that Airflow
installations tend to occupy. This project's own Airflow UI is on `8082` (`AIRFLOW_PORT`).

Start it with:

```bash
docker compose --profile geocoding up -d nominatim
docker compose logs -f nominatim
```

Expect the Berlin import to take roughly 20 to 30 minutes.

Set:

```bash
NOMINATIM_URL=http://localhost:8081/reverse
```

## Pipeline usage

For local runs:

```bash
OVERPASS_URL=http://localhost:12345/api/interpreter \
MAX_RECORDS=200 \
OUTPUT_FILE=gewerbedaten.json \
.venv/bin/python main.py
```

Or export the env vars in your shell:

```bash
export OVERPASS_URL=http://localhost:12345/api/interpreter
export NOMINATIM_URL=http://localhost:8081/reverse
```

With both services self-hosted, you can also reduce delays:

```bash
export OVERPASS_DELAY_S=0
export NOMINATIM_DELAY_S=0
```

## Env vars summary

| Variable | Default | Description |
| --- | --- | --- |
| `OVERPASS_URL` | `https://overpass-api.de/api/interpreter` | Primary Overpass endpoint used by the pipeline unless overridden |
| `OVERPASS_RADIUS` | `80` | Search radius in metres |
| `OVERPASS_DELAY_S` | `1.5` | Delay between calls; use `0` for local self-hosted |
| `NOMINATIM_URL` | `https://nominatim.openstreetmap.org/reverse` | Reverse geocoding endpoint unless overridden |
| `NOMINATIM_DELAY_S` | `2.0` | Delay between calls; use `0` for local self-hosted |

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `504 Gateway Timeout` on public Overpass | Public servers are overloaded; use the self-hosted service |
| `429 Too Many Requests` from Nominatim | Increase `NOMINATIM_DELAY_S` or use self-hosted Nominatim |
| Import stuck or container OOM | Increase Docker memory to at least 4 GB |
| `Dispatcher not found` | Overpass import is still running; wait for `Dispatcher started.` |
| Old data after restart | Set `OVERPASS_MODE=clone` to enable diff updates |
