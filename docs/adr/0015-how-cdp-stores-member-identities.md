# ADR-0015: How CDP stores member identities

**Date**: 2026-07-28
**Status**: accepted
**Deciders**: Yeganathan S

## Context

CDP treats member identities as case-insensitive when resolving people (`lower(value)` in lookups and many DAL queries), which matches how major platforms behave:

- **GitHub / GitLab**: usernames are case-insensitive for uniqueness, but case-preserving for display (`WillsonHG` and `willsonhg` are the same account; APIs return preferred casing).
- **Discord** (new usernames): forced lowercase.
- **Email**: stored and compared lowercase in practice.

Historically, `memberIdentities` uniqueness was defined on raw `value` (case-sensitive):

- `uix_memberIdentities_memberId_platform_value_type`
- `uix_memberIdentities_platform_value_type_verified`

Lookups used `lower(value)`, but inserts often did not. Data-sink `mergeData` matched with exact `value ===`, so a self-serve lowercase `willsonhg` plus a later GitHub ingest of `WillsonHG` produced two rows for the same identity — often both verified on the same member. Prod had tens of thousands of GitHub case-variant groups.

Soft-deleting or auto-verifying case variants only at verification time would treat a write-path bug as a product special case.

## Decision

**Mental model**

| Kind     | Store                            | Compare / unique on             |
| -------- | -------------------------------- | ------------------------------- |
| username | preferred casing from the source | `lower(value)`                  |
| email    | always lowercase                 | `lower(value)` (same as stored) |

Identity equality in CDP is `(platform, type, lower(value))`. Email vs username for storage casing is inferred from the value with `isValidEmail`, not from `type` (git often stores emails as `type=username`). Non-email usernames keep preferred casing from the source.

**Enforcement**

1. **Write paths** match and upsert with case-insensitive equality (`isSameMemberIdentity` / `lower(value)`). Do not insert a second row that only differs by casing.
2. **DB uniqueness** uses expression unique indexes on `lower(value)` (partial on `deletedAt is null`, and verified-only for the global verified owner index). See migration `V1785255019__member_identities_case_insensitive_unique_indexes.sql`.
3. **Existing duplicates** are cleaned with a one-time script before the unique indexes can be applied: same-member case variants → keep one (prefer verified + `verifiedBy`, else most recent integration casing) and soft-delete the rest; cross-member unverified variants of a verified identity → soft-delete the unverified; both verified across members → merge / existing capitalization-merge workflows, not blind soft-delete.
4. **Verification** does not need special “soft-delete case siblings” logic once the invariant holds — verifying finds the one row.

## Alternatives Considered

### Alternative 1: Soft-delete / auto-verify case variants at identity verification time

- **Pros**: Fixes the user-visible self-serve pain quickly; no schema change.
- **Cons**: Case variants keep being inserted by ingest/enrichment; verify path becomes a mop; duplicates still break uniqueness and analytics.
- **Why not**: Papers over the root cause. If case variants should not exist, stop creating them and clean existing data.

### Alternative 2: Always store usernames lowercase (like emails / Discord)

- **Pros**: Simplest storage; uniqueness on `value` works without expression indexes.
- **Cons**: Throws away GitHub/GitLab preferred casing; diverges from source payloads; confuses display and support (“CDP shows lowercase but GitHub shows mixed”).
- **Why not**: We want GitHub-style case-preserving storage. Uniqueness belongs on `lower(value)`, not on mutating the stored handle.

### Alternative 3: Keep case-sensitive unique indexes; only fix app-layer matching

- **Pros**: No migration; no cleanup required to change indexes.
- **Cons**: App bugs or races can still insert case variants; DB does not enforce the domain invariant.
- **Why not**: At this scale, durable invariants need to live in the database, not only in callers.

### Alternative 4: Update stored casing on every ingest when preferred casing differs

- **Pros**: `value` always mirrors latest source casing.
- **Cons**: Unsafe while same-member case-variant pairs still exist (updating both rows to the same `value` hits the old unique index). Extra write noise.
- **Why not**: Deferred until after cleanup. Preventing duplicate inserts is enough for the durable fix; optional casing refresh can come later.

## Consequences

### Positive

- One clear rule: same platform + type + lower(value) ⇒ same identity.
- Lookups, writes, and uniqueness agree.
- Self-serve / GitHub / enrichment stop creating `WillsonHG` + `willsonhg` pairs.
- Preferred username casing from integrations is preserved.

### Negative

- Cleanup must run before the unique-index migration, or `create unique index` fails (and can leave an `INVALID` index).
- Expression unique indexes are slightly less obvious than column-only uniques; callers must keep using `lower(value)` (or `isSameMemberIdentity`) consistently.
- Conflict handlers need to recognize both old and new constraint names during rollout.

### Risks

- **Migration applied before cleanup** — mitigated by documenting order: write-path fix → cleanup script → unique-index migration.
- **Cross-member verified case variants** — rare; require merge, not soft-delete. Existing `findAndMergeMembersWithSamePlatformIdentitiesDifferentCapitalization` covers part of this.
- **Incomplete write-path coverage** — mitigated by DB unique indexes as the backstop once cleanup is done; shared `isSameMemberIdentity` for app equality.
