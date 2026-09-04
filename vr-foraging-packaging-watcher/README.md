# VR-Foraging packaging watcher

Watches `\\allen\aind\stage\vr-foraging\data` for new sessions (for a
configured list of subjects), packages each one, and uploads the result to
`s3://aind-scratch-data/vr-foraging/replenishment-temp-sharing/<session_name>/`.

## Prerequisites

- Docker + Docker Compose on the host
- The network share reachable from the host (see step 1 below)
- AWS credentials with `s3:PutObject`/`s3:ListBucket` on the destination prefix

## Deploy

**1. Get the share onto the host.**

Linux — add to `/etc/fstab` and mount:

```
//allen/aind/stage/vr-foraging/data  /mnt/vr-foraging-data  cifs  ro,credentials=/etc/cifs-vrforaging,vers=3.0,uid=0,gid=0,_netdev  0  0
```

Windows / Docker Desktop — a UNC path can't be bind-mounted, so let Docker mount
it instead: uncomment the `raw:` CIFS volume at the bottom of
[docker-compose.yml](docker-compose.yml), replace the `${HOST_RAW_DIR}:/data/raw:ro`
line with `raw:/data/raw:ro`, and set `SMB_USER`/`SMB_PASSWORD`/`SMB_DOMAIN` in
`.env` (step 2).

**2. Set up `.env`** (host paths and secrets only — never committed):

```bash
cp .env.example .env
```

Edit `HOST_RAW_DIR` (the mount from step 1) and `HOST_AWS_DIR` (a folder holding
an AWS `credentials` file, e.g. your `~/.aws`).

**3. Edit the settings in [docker-compose.yml](docker-compose.yml).** Everything
that isn't a host path or a secret lives in its `environment:` block. At minimum,
set `VRFW_SUBJECTS` — it defaults to a real subject list, but confirm it's the
one you want; an empty value means *every* subject.

**4. Dry-run it** — confirms which sessions are in scope without packaging or
uploading anything:

```bash
docker compose run --rm -e VRFW_DRY_RUN=true -e RUN_MODE=once packaging-watcher
```

**5. Deploy for real:**

```bash
docker compose up -d --build
```

## Viewing logs

```bash
docker compose logs -f           # follow live
docker compose logs --since 1h   # recent history
```

Other useful checks:

```bash
docker compose run --rm packaging-watcher --status        # the ledger: what's done/pending/failed
docker compose run --rm packaging-watcher --show-config    # resolved settings
docker inspect --format '{{.State.Health.Status}}' vr-foraging-packaging-watcher   # healthy/unhealthy
```

A session that keeps failing is parked as `failed` in the ledger with its error
attached (visible via `--status`) and is not retried further — clear its entry
in `sessions.json` (in the `state` volume) to retry it.

## Changing a setting

Edit [docker-compose.yml](docker-compose.yml), then:

```bash
docker compose up -d --build
```

## Stopping

```bash
docker compose down       # stops the container; the state/scratch volumes are kept
docker compose down -v    # also wipes the ledger -- next start re-checks S3 before re-uploading anything
```
