# Sched historical import

A one-time, re-runnable import of past LF Sched events into CDP
([ADR-0037](../../../../../docs/adr/0037-sched-historical-import-script.md)). For each event in
[`events.json`](events.json) it reads the Sched API and sends `registered-session` and
`spoke-at-session` activities to the data sink, into the event's project. The first run for a
project creates its `sched` integration.

## Before a run

- **Events:** add the event to `events.json` (subdomain, display name, and the project's subproject
  segment `slug` and `sourceId`) and merge it like code.
- **Keys:** each event's API key is a concealed field, labelled with the subdomain, in the
  `Sched import keys` item of the `CM credentials` 1Password vault. The script reads them from
  stdin and never stores or logs them.
- **Access:** 1Password and `kubectl` access to the cluster.

## Running

Keys go from 1Password through `jq` straight into the pod, so they never touch disk:

```bash
POD=$(kubectl get pods -o name | grep connectors-worker | head -1)

op item get "Sched import keys" --vault "CM credentials" --format json \
  | jq -r '.fields[] | select(.type == "CONCEALED") | "\(.label)=\(.value)"' \
  | kubectl exec -i "$POD" -- sh -c \
    'cd services/apps/connectors_worker && pnpm run script:import-sched --keys-stdin --events kccnceu2021 --dry-run'
```

| Flags          | What it does                                                                                              |
| -------------- | --------------------------------------------------------------------------------------------------------- |
| `--check-keys` | One `user/list` call per event: `ok-rw` (returns emails), `ok-ro` (no emails) or `bad key`. Nothing sent. |
| `--dry-run`    | Reads and transforms each event and logs the counts. Nothing sent, no integration created.                |
| none           | Imports the events.                                                                                       |
| `--events a,b` | Limits any of the above to those events; the default is every event in `events.json`.                     |

Order: check the keys, dry-run the event, import `kccnceu2021` and check it in CDP, then the rest
in batches with `--events`, largest first.

## Limits

- Calls are paced at one every 5 seconds (12 a minute) across all keys; Sched revokes keys that go
  over 30 a minute. A rate-limit response stops the whole run.
- An event takes 5 calls (about 25 seconds plus the transfer); the registration list of a large
  event is about 50 MB.
- The run ends with the events done, skipped (no key, bad key) and failed. Re-running failed
  events is safe: an activity with the same timestamp, type, source id, channel and project is
  updated, not duplicated.
- Registration time is the session start (`DEFAULT_TRANSFORM_OPTIONS` in `transform.ts`), so it
  stays the same between runs.
