# ADR-0015: How CDP stores member identities

**Date**: 2026-07-28
**Status**: accepted
**Deciders**: Yeganathan S

## Context

Member identities in CDP come from many sources (integrations, enrichment, UI, public APIs). The same logical identity often arrives with different casing — for example GitHub’s `login` is case-preserving in the API but case-insensitive for account uniqueness.

CDP already resolves people with case-insensitive lookups (`lower(value)`). Uniqueness and some write paths historically used raw `value`, so the same identity could be stored more than once under different casings. Lookups, merges, and verification then disagree about whether two rows are “the same.”

We need a single, durable rule for what we store and what counts as the same identity — aligned with how the platforms we integrate with actually work.

## Decision

Identity equality in CDP is `(platform, type, lower(value))`.

| Kind     | Store in `value`                             | Compare / unique on |
| -------- | -------------------------------------------- | ------------------- |
| username | preferred casing from the source/integration | `lower(value)`      |
| email    | always lowercase                             | `lower(value)`      |

Whether a value is stored as an email (lowercased) vs a username (preferred casing) is inferred from the string with `isValidEmail`, not only from `type` — some sources (notably git) store email addresses with `type = username`.

### Conventions

1. **Do not invent casing** — for non-email usernames, persist what the source sent (after trim). Do not force lowercase on GitHub/GitLab-style handles.
2. **Do not insert case variants** — if a row already exists for the same `(platform, type, lower(value))`, update or no-op; never insert a second row that only differs by casing.
3. **App equality matches DB equality** — use `isSameMemberIdentity` (or equivalent `lower(value)` comparisons) on write and merge paths. `type` stays in the equality key even when a git `username` holds an email-shaped string.
4. **DB enforces the invariant** — unique indexes on `lower(value)` (per member for all active identities; globally for verified identities). Schema detail: migration `V1785255019__member_identities_case_insensitive_unique_indexes.sql`.

## Alternatives Considered

### Alternative 1: Always store usernames lowercase

- **Pros**: Simplest storage; uniqueness can stay on raw `value`.
- **Cons**: Discards preferred casing from GitHub/GitLab; CDP display and support diverge from the source platform.
- **Why not**: We want GitHub-style case-preserving storage. Sameness belongs on `lower(value)`, not on rewriting the handle.

### Alternative 2: Case-sensitive storage and uniqueness (raw `value` only)

- **Pros**: Matches naive string equality; no expression indexes.
- **Cons**: Conflicts with how platforms define accounts and with how CDP already looks identities up; duplicate rows for the same person identity are inevitable.
- **Why not**: That mismatch is the problem this decision closes.

### Alternative 3: Case-insensitive matching in application code only

- **Pros**: No schema change.
- **Cons**: Any missed caller or race can still insert case variants; the database does not protect the invariant.
- **Why not**: At CDP scale, identity uniqueness must be enforced in the database as well as in callers.

### Alternative 4: Refresh stored casing on every ingest when the source casing differs

- **Pros**: `value` always mirrors the latest source spelling.
- **Cons**: Extra writes; needs a single surviving row per identity before it is safe.
- **Why not**: Optional later enhancement. Preventing duplicate rows is the required invariant; updating preferred casing can be layered on afterward.

## Consequences

### Positive

- One rule for “same identity” across lookup, ingest, merge, and uniqueness.
- Preferred username casing from integrations is preserved.
- Emails are normalized consistently.

### Negative

- Callers must compare with `lower(value)` / `isSameMemberIdentity`, not exact string equality.
- Unique indexes are expression-based (`lower(value)`), which is slightly less obvious than column-only uniques.

### Risks

- **Missed write path still using exact `value ===`** — mitigated by DB unique indexes on `lower(value)` and shared helpers.
- **Two verified members sharing the same identity under `lower(value)`** — a data conflict requiring merge, not a second identity row.
