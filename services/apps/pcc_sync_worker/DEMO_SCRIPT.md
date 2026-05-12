# PCC Sync — Demo Video Script

---

## INTRO (~15s)

> "I want to show you something we built that completely removed a manual process from the team's workflow. It's about keeping project metadata in sync across our platform — automatically."

---

## SCENE: What is PCC (~20s)

> "PCC — the Linux Foundation's Project Control Center — is where project teams manage their projects. Things like the project name, status, maturity level, description. When a project gets archived, or goes through incubation, that change happens on PCC."

---

## SCENE: The old pain (~30s)

> "The problem was: CDP and LFX Insights had no idea. Every time something changed on PCC, someone had to file a support request. Then one of our engineers had to go into the CDP admin UI, search for the project, open the form, manually type in the new values, and save. And then do the same thing in Insights separately. Days of lag, error-prone, and honestly — just toil nobody should be doing."

---

## SCENE: The new system (~20s)

> "So we built the PCC sync worker. It runs every day at 1 AM UTC. No tickets. No UI. No humans."

---

## SCENE: Walk through the pipeline (~40s)

> "Here's what happens. Every night, we export the full project hierarchy from Snowflake — that's all the leaf projects, their ancestry, their current status. It lands as a Parquet file in S3. Our consumer picks it up, parses each project, matches it to the corresponding segment in CDP, and writes the updated metadata in a single transaction. Name, status, maturity, description, logo — all synced."

---

## SCENE: How we match PCC projects to CDP segments (~30s)

> "One of the trickier parts was figuring out: for a given PCC project, which record in our database does it correspond to? PCC and CDP evolved independently, so there's no guaranteed shared key.
>
> We use a two-step matching cascade. First, the Snowflake export itself joins against our active segments table and includes the CDP segment ID directly — so for the majority of projects we get an exact UUID match, no guesswork.
>
> For projects where that join comes up empty — maybe the segment was added after the last export, or the mapping isn't there yet — we fall back to matching on `sourceId`. That's a field on our segments table we populate with the PCC project ID, so we can look it up directly.
>
> If neither step finds a match, we skip the project for now. Phase one of this work is about keeping existing projects in sync — onboarding new ones is a separate problem. And if a match is ambiguous — two CDP subprojects somehow sharing the same PCC ID — we log it as a diagnostic and skip rather than risk writing to the wrong record."

---

## SCENE: The archival example — key demo point (~30s)

> "A good concrete example is project status. When a project gets archived on PCC, that status change flows through the pipeline and updates the `status` field directly in our database — no manual step. CDP reflects it within 24 hours. LFX Insights picks it up within another hour after that. So the moment someone marks a project Archived on PCC, every downstream product stays consistent automatically. Same goes for projects moving into Formation stage, or becoming active — any status transition is picked up on the next nightly run."

---

## SCENE: Diagnostics / edge cases (~20s)

> "We also track edge cases — things like hierarchy mismatches or slug drift — so the team has full visibility. But the happy path: zero intervention. The data just stays consistent."

---

## OUTRO (~15s)

> "What used to be a support request, a manual form, and days of lag — is now a scheduled job running silently every night. The human is completely out of the loop, and every LFX product that depends on this data stays in sync automatically."

---

**Total: ~2.5 min.** Trim the Snowflake/Parquet detail if running short.

---

## Technical reference (for Q&A)

| What changes on PCC | What gets updated in CDP |
|---|---|
| Project name | `segments.name`, `insightsProjects.name` |
| Status (Active / Archived / Formation / Prospect) | `segments.status` |
| Maturity level | `segments.maturity` |
| Description | `segments.description`, `insightsProjects.description` |
| Logo | `insightsProjects.logoUrl` (only fills if missing) |

**Frequency:** Daily at 01:00 UTC → CDP updated same morning → Insights within ~1 h.

**Status mapping:** PCC `PROJECT_STATUS` → CDP `segmentsStatus_type` enum. Archived on PCC = `archived` in CDP. All status transitions are picked up on the next nightly run.

**Matching cascade:**
1. **Segment ID from Snowflake** — export JOIN against `ACTIVE_SEGMENTS` includes the CDP `segment_id` directly. Exact UUID match, no ambiguity.
2. **sourceId fallback** — if step 1 misses, look up `segments WHERE sourceId = PCC_PROJECT_ID AND type = 'subproject'`. Works because `sourceId` is populated with the PCC project ID when segments are created.
3. **No match → skip** — project not yet in CDP; Phase 1 scope is syncing existing projects only.
4. **Ambiguous match → skip + log** — two subproject segments share the same `sourceId` (data anomaly); recorded in `pcc_projects_sync_errors` for triage.
