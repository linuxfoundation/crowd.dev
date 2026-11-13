# Sched historical import

A one-time, re-runnable import of past LF Sched events into CDP
([ADR-0037](../../../../../docs/adr/0037-sched-historical-import-script.md)). For each event it
receives, it reads the Sched API and sends `registered-session` and `spoke-at-session` activities
to the data sink, into the event's project. The first run for a project creates its `sched`
integration.

## Inputs

Both live in the `CM credentials` 1Password vault, one field per event labelled with the event's
subdomain. Neither is in this repository, and the script only reads them from stdin.

| Item                  | Field type | Field value                                                                               |
| --------------------- | ---------- | ----------------------------------------------------------------------------------------- |
| `Sched import keys`   | concealed  | the event's API key                                                                       |
| `Sched import events` | text       | `{"name":"<display name>","slug":"<subproject slug>","sourceId":"<subproject sourceId>"}` |

`slug` and `sourceId` identify the CDP subproject segment the event's activities go to. To add or
change an event:

```bash
op item edit "Sched import events" --vault "CM credentials" \
  '<subdomain>[text]={"name":"<display name>","slug":"<slug>","sourceId":"<sourceId>"}'
```

An event is imported only when both items have a field for it. Dry-run an event after changing
its project: a project change after import creates duplicates in the new project.

## Running

The two items go from 1Password through `jq` straight into the pod, so nothing touches disk. Events
become one JSON line each, keys one `subdomain=key` line each:

```bash
VAULT="CM credentials"
POD=$(kubectl get pods -o name | grep connectors-worker | head -1)

{
  op item get "Sched import events" --vault "$VAULT" --format json \
    | jq -c '.fields[] | select(.type == "STRING" and .purpose != "NOTES")
        | {subdomain: .label} + (.value | fromjson)'
  op item get "Sched import keys" --vault "$VAULT" --format json \
    | jq -r '.fields[] | select(.type == "CONCEALED") | "\(.label)=\(.value)"'
} | kubectl exec -i "$POD" -- sh -c \
  'cd services/apps/connectors_worker && pnpm run script:import-sched --stdin --events kccnceu2021 --dry-run'
```

| Flags          | What it does                                                                                              |
| -------------- | --------------------------------------------------------------------------------------------------------- |
| `--check-keys` | One `user/list` call per event: `ok-rw` (returns emails), `ok-ro` (no emails) or `bad key`. Nothing sent. |
| `--dry-run`    | Reads and transforms each event and logs the counts. Nothing sent, no integration created.                |
| none           | Imports the events.                                                                                       |
| `--events a,b` | Limits any of the above to those events; the default is every event received.                             |

Order: check the keys, dry-run the event, import `kccnceu2021` and check it in CDP, then the rest
in batches with `--events`, largest first.

## Limits

- Calls are paced at one every 5 seconds (12 a minute) across all keys; Sched revokes keys that go
  over 30 a minute. A rate-limit response stops the whole run.
- An event takes 5 calls (about 25 seconds plus the transfer); the registration list of a large
  event is about 50 MB, parsed in memory.
- The run ends with the events done, skipped (no key, bad key) and failed. Re-running failed
  events is safe: an activity with the same timestamp, type, source id, channel and project is
  updated, not duplicated.
- Registration time is the session start (`DEFAULT_TRANSFORM_OPTIONS` in `transform.ts`), so it
  stays the same between runs.
