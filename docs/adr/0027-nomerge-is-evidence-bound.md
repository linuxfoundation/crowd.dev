# ADR-0027: noMerge is a veto against evidence, not against member IDs forever

**Date**: 2026-09-16
**Status**: accepted
**Deciders**: Yeganathan S

## Context

`memberNoMerge` records that two members were judged to be different people. Once written, the pair is never suggested or auto-merged again.

The veto used to be permanent. Identity evidence is not. Two profiles with similar names and different handles on different platforms can be correctly rejected, then later both gain the same verified email from different sources. Verified identity uniqueness is per platform, so the conflict path does not reopen them. The old noMerge still applies. We never look again.

Clearing every noMerge row on any identity write would reopen correctly rejected pairs — ingest and enrichment are noisy with unverified identities. It would also fight unmerge, which writes identities and then writes noMerge in that order.

## Decision

noMerge means "these two were judged different given evidence E," not "these two IDs are different forever." When E changes in a merge-relevant way, the veto is stale.

**Evidence-bound veto.** When we write noMerge, snapshot both members' verified identity sets (platform, type, lowercased value) as a hash on the row. When we read noMerge, ignore the row if the hash no longer matches the current verified identity set. If a human re-rejects, rewrite the snapshot so it sticks.

Rows that predate the hash: expire only when the two members now share a verified email. Same username on two different platforms is not enough.

Unmerge is a normal insert. Identities are split first, then noMerge is written against the post-split state, so the snapshot reflects the split and the veto holds.

**Re-queue signal.** Identity writes bump `members.updatedAt` when a row actually changes. The merge-suggestions generator pages members with `updatedAt` after the last run. Some identity paths already bumped the member; others (enrichment, verify-flag flips, identity moves) did not. The identity write itself updates the parent timestamp — parent mtime, not merge policy.

## Alternatives Considered

### Alternative 1: Clear all noMerge for a member on every identity create / update / delete

- **Pros**: Simple; any identity change reopens every pair.
- **Cons**: Noisy identity writes reopen correctly rejected pairs. Unmerge wipes unrelated vetoes on the primary. Identity storage starts owning merge policy.
- **Why not**: Too broad, and the side effect lives in the wrong place.

### Alternative 2: Ignore noMerge when the pair currently shares an email, no snapshot

- **Pros**: Fixes the shared-email case with no new column.
- **Cons**: Human says no, they still share the email, the pair comes back forever.
- **Why not**: Without a snapshot of what the "no" was based on, a shared email makes the veto impossible to keep.

### Alternative 3: Delete LLM verdicts so auto-merge retries

- **Pros**: The model could look at the pair again with the new identities.
- **Cons**: Verdicts are an audit log. Deleting them to unblock a worker treats a log as a lock. A retry is not free and not guaranteed to say yes.
- **Why not**: Reopened pairs go to humans. If we later want the model to retry, ignore a stale verdict, do not delete it.

### Alternative 4: Bump `members.updatedAt` at every identity call site

- **Pros**: Identity DAL stays a pure identity write.
- **Cons**: UI and public API remembered; enrichment, verify-flag flips, and identity moves did not. The next writer will miss it too.
- **Why not**: The forgotten callers are why members with new identities never re-entered the generator. The invariant belongs next to the write.

### Alternative 5: Postgres trigger on identities to bump the member

- **Pros**: Impossible to forget; no app code.
- **Cons**: Hidden. Backfills and tests get harder to reason about.
- **Why not**: Same outcome as touching from the write, with less visibility.

### Alternative 6: Generator reads identity timestamps instead of copying mtime onto the member

- **Pros**: Identity rows already have `createdAt` / `updatedAt` / `deletedAt`. No parent bump, no redundant updates, no "forgot to touch."
- **Cons**: The watermark query has to consider child rows. Other consumers already key off `members.updatedAt`.
- **Why not**: Cleaner long-term. We did not do it this round. The product change was the veto, not a new way to find dirty members. Do not remove the parent touches without this alternative in mind.

## Consequences

### Positive

- A later verified email can reopen a pair rejected on weaker evidence, without reopening every noisy identity write.
- Unmerge needs no special flag or ordering trick.
- Identity storage does not know about noMerge. Merge code does not hook identity writes.
- The generator sees identity changes even on paths that never updated the member row before.

### Negative

- One extra column on `memberNoMerge`, and a hash check on every noMerge read.
- Identity writes also update the member row. Several writes in one request bump the same timestamp more than once. Harmless (same transaction, same `now()`, generator still queues the member once), but it is a copy of information the identity row already has.

### Risks

- **Legacy rows without a hash.** Mitigated by only expiring those when the pair shares a verified email, not on any identity overlap.
- **Someone removes the parent-row bump as redundant.** The generator would stop seeing identity-only changes. Alternative 6 is the real replacement, not deleting the bump.
- **High-activity members** get unverified identities constantly. Those do not change the verified-set hash, so noMerge still holds. They do bump `updatedAt`, so the generator may look at the member again — extra work, not wrong merges.
