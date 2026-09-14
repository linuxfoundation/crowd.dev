# ADR-0023: CNCF `maintainers.yaml` as authoritative, self-correcting source

**Date**: 2026-09-10
**Status**: accepted
**Deciders**: Mouad Bani

## Context

`MaintainerService.extract_maintainers` (`services/apps/git_integration/src/crowdgit/services/maintainer/maintainer_service.py`) treats every candidate maintainer file — `CODEOWNERS`, `MAINTAINERS.md`, `README.md`, `maintainers.yaml`, etc. — as equally valid input. Candidates are scored and classified by an AI gate, then unioned: every root candidate that passes is analyzed and its entries appended to a combined list (`combined_info`), with the single file that yielded the most entries saved as `repositoryProcessing.maintainerFile`. Once a file is saved, every later run short-circuits through `try_saved_maintainer_file` and only re-reads that one file — the other candidates are never revisited.

CNCF member projects increasingly publish a structured `maintainers.yaml`/`.yml` following the CNCF governance format, intended as the authoritative maintainer roster. Under the existing logic, `maintainers.yaml` had no special standing: it was one candidate among several, and which file "won" depended on raw extracted-entry count, not format or authority. An audit of 202 CNCF `.project`-pattern repos found only 59 (29%) had settled on `maintainers.yaml`; the remaining 143 had permanently locked onto `CODEOWNERS` or another file the first time that file happened to extract more entries, and stayed there indefinitely.

## Decision

For repos matching the CNCF `.project` convention, run a new Step 0 ahead of the existing saved-file shortcut: locate a CNCF-format `maintainers.yaml`/`.yml` via `find_cncf_maintainers_file`, parse it with `parse_cncf_maintainers_yaml`, and if it parses to a non-empty roster, return it immediately as the maintainer result — bypassing the union-and-count logic and overriding whatever file was previously saved. If no CNCF-format file is found or it fails to parse, fall through unchanged to the existing saved-file / candidate-search / AI-detection pipeline. Because this check runs on every eligible run (not just the first), the saved file self-corrects toward `maintainers.yaml` even for repos already locked onto a different file.

## Alternatives Considered

### Alternative 1: Prioritize `maintainers.yaml` inside the existing candidate union
- **Pros**: Smaller change; reuses the existing scoring/classifier pipeline; no new CNCF-specific code path.
- **Cons**: Still subject to the saved-file shortcut — a repo already locked onto `CODEOWNERS` would never re-run detection to discover the higher-priority file exists.
- **Why not**: Doesn't fix the actual bug (permanent lock-in); would require also changing the shortcut logic for every repo, not just CNCF ones, expanding blast radius.

### Alternative 2: One-time backfill migration instead of a code change
- **Pros**: No runtime logic change; a single script could re-detect and re-save the correct file for all currently-affected repos.
- **Cons**: Doesn't self-correct going forward — any repo that adds `maintainers.yaml` later, or any newly onboarded CNCF repo, would hit the same bug again.
- **Why not**: Treats a systemic bug as a one-off data-quality issue; the union/count-based selection was still wrong for this repo class.

## Consequences

### Positive
- CNCF `.project` repos now resolve to the CNCF-authoritative roster regardless of what any other candidate file contains or how many entries it extracts.
- Self-correcting: a repo already locked onto a stale file recovers on its next processing run, no manual reset required going forward.
- Reprocessing all 202 CNCF `.project` repos raised `maintainers.yaml` adoption from 59/202 (29%) to 200/202 (99%); 141 repos switched their saved file (mostly `CODEOWNERS` → `maintainers.yaml`), and 42 repos gained previously-missing maintainers with none lost.

### Negative
- Adds a CNCF-specific branch to a service that was otherwise format-agnostic, increasing the number of code paths to reason about in `extract_maintainers`.
- Repos are re-checked against the CNCF path on every eligible run instead of only once, adding a bounded amount of extra file-system/parse work per run for CNCF repos specifically.

### Risks
- If a CNCF repo's `maintainers.yaml` is malformed or temporarily wrong, the self-correction could overwrite a previously-correct saved file; mitigated by only overriding when `parse_cncf_maintainers_yaml` returns a non-empty roster, and by keeping the existing pipeline as a fallback when parsing fails.
- The remaining 2/202 repos (`arras-energy`, `sdcio`) still resolve to no maintainer file after reprocessing — genuinely empty upstream data, not a code issue, but worth tracking if it recurs across other repos.
