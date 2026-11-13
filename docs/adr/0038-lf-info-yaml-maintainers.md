# ADR-0038: LF Release Engineering `INFO.yaml` as authoritative maintainer source

**Date**: 2026-11-12
**Status**: accepted
**Deciders**: Joana Maia

## Context

LF-hosted Gerrit projects (ONAP, OpenDaylight, FD.io, O-RAN-SC and others run by LF Release Engineering) list their project lead and committers in a root `INFO.yaml` that follows the [LF Release Engineering schema](https://github.com/lfit/releng-global-jjb/blob/master/schema/info-schema.yaml). `MaintainerService.extract_maintainers` never considered it: `info` is not a governance file name, so these repositories either had no maintainer file or settled on `AUTHORS`, `CONTRIBUTORS` or a `README`, and the saved-file shortcut kept them there (140 of 162 FD.io repositories and 88 of 196 OpenDaylight ones). Committers were therefore not tagged as maintainers on the contributor leaderboard and did not count as curated maintainers for bus factor (reported in linuxfoundation/insights#2480, tracked in linuxfoundation/lfx-cdp-insights-ops#1131). The file also carries a `tsc.changes` log of approved additions and removals, so not every person named in it is a current committer.

## Decision

Treat a root `INFO.yaml` that has a `project_lead` object and a `committers` list as the authoritative maintainer roster: parse it directly, without the LLM, and record the project lead (role "Project Lead") and every committer (role "Committer") as maintainers, ignoring `tsc`. The check runs after the CNCF step from ADR-0023 and before the saved-file shortcut, so repositories already locked onto another file switch on their next run; a file that doesn't match the format falls through to the existing pipeline.

## Alternatives Considered

### Alternative 1: Add `INFO.yaml` to the known governance file names

- **Pros**: One-line change; reuses the existing candidate, classification and extraction pipeline.
- **Cons**: The extraction prompt maps roles by file name and sends anything that isn't `MAINTAINERS`, `CODEOWNERS`, `OWNERS` or `REVIEWERS` to "contributor", so committers would not be maintainers. The prompt extracts every person named, which would pull removed committers out of `tsc.changes`. Repositories with a saved file would never reach it, and it ties with `README.md` on score.
- **Why not**: It would not tag committers as maintainers for most of the affected repositories.

### Alternative 2: Read committers from Gerrit group membership

- **Pros**: The live source of submit rights, independent of a file in the repository.
- **Cons**: Needs authenticated access to each Gerrit instance's groups API, which instances don't expose anonymously, and a new per-instance integration.
- **Why not**: `INFO.yaml` is the public, LF-maintained mirror of the same list and is already in every clone.

## Consequences

### Positive

- Committers of LF Release Engineering projects are tagged as maintainers with no AI cost, and the roster updates when the file does; removed committers are end-dated by the existing comparison.
- Repositories stuck on `AUTHORS` or `CONTRIBUTORS` self-correct without a data migration.

### Negative

- GitHub mirrors carry the same `INFO.yaml` and receive the same maintainers. Since mirrors only contribute to the security category of the project Health Score, this only duplicates leaderboard tags across the Gerrit repository and its mirror.
- Committers without a resolvable email are skipped, as for any other maintainer file.

### Risks

- A repository outside LF Release Engineering could use an `INFO.yaml` with the same two keys for something else. The format check requires both keys with the right types, which keeps this unlikely; a mismatch falls back to normal detection.
- Projects that let `INFO.yaml` go stale keep stale committers. Bus factor only counts maintainers with activity in the last 12 months, which limits the effect on scores.
