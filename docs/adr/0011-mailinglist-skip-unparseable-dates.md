# ADR-0011: Skip Mailing List Activities With Unparseable/Implausible Dates

**Date**: 2026-07-19
**Status**: accepted
**Deciders**: Uros Marolt

## Context

The mailing list integration (`services/apps/mailing_list_integration`) parses
RFC2822 `Date` headers from archived mailing list messages
(`crowdmail/services/parse/noteren.py`, `parse_email()`) to build each
activity's `timestamp`. During testing against a real archive
(`linux-serial`, mirrored from lore.kernel.org), a message surfaced whose
`Date` header parsed "successfully" via `email.utils.parsedate_tz()` but
produced an implausible year (e.g. `102` instead of a 4-digit year).
Python's stdlib already RFC2822-normalizes legitimate 2-digit years (`68` →
`2068`/`02` → `2002`), so this is not a parsing bug in our port — the
`Date` header itself is malformed in the source archive. Without a guard,
`mktime_tz()` + `strftime("%Y-...")` emits a non-4-digit year (e.g.
`"102-06-12T11:56:20.000000Z"`), which downstream JS `Date` parsing (in
the Node ingestion pipeline) rejects, causing per-message ingestion errors.

## Decision

Reject any parsed date whose year falls outside `1970–9999` by raising
`ValueError` (caught by the existing `except (ValueError, OverflowError)`
block), leaving `timestamp` empty for that message. `list_worker.py` skips
queuing any activity with an empty timestamp. The message is not ingested
as an activity, and its shard head still advances past it (so it is not
retried on subsequent polls).

## Alternatives Considered

### Alternative 1: Heuristically repair malformed years

- **Pros**: Could recover activities that would otherwise be dropped.
- **Cons**: No reliable signal for what year was intended once the header
  is already malformed at the source (e.g. `102` — off by 1900? 2000?
  truncated?).
- **Why not**: Any heuristic risks silently mis-dating an activity, which
  is worse than dropping it — a wrong timestamp corrupts activity
  timelines and trend analytics without any visible error.

### Alternative 2: Ingest with the corrupted timestamp as-is

- **Pros**: No data loss.
- **Cons**: Breaks downstream JS `Date` parsing in the ingestion pipeline,
  causing hard failures rather than a clean skip.
- **Why not**: A hard pipeline failure for one bad message is worse than
  skipping that one message.

## Consequences

### Positive

- Malformed dates never reach the ingestion pipeline as corrupt
  timestamps; no pipeline-level failures from a single bad message.
- Matches the existing skip pattern already used for other
  unparseable/missing fields in this parser.

### Negative

- Messages with malformed `Date` headers are silently and permanently
  dropped — never retried, since the shard head advances past them
  regardless. The only visible trace is a `logging.warning()` line.

### Risks

- If a source archive has a systemic date-formatting issue (not just rare
  garbage), this could silently drop a meaningful fraction of activities.
  Mitigation: the warning log includes the raw header and commit id, so a
  spike can be diagnosed via log volume if ever suspected.
