# Affiliation rules

How CDP picks the organization an activity is credited to. The same rules run in three places:
at ingest in `data_sink_worker`, in the recalculation in this module
(`refreshMemberOrganizationAffiliations`), and in the public affiliations API, which uses
`../affiliations/index.ts`. How those paths connect is in
[`docs/design/affiliation.md`](../../../../../docs/design/affiliation.md); where each piece lives is
in [`docs/knowledge/flow-affiliation.md`](../../../../../docs/knowledge/flow-affiliation.md).

## Resolution at ingest

`data_sink_worker` resolves the organization once per new activity and stores it with the
activity. Bot members get `null`.

```mermaid
flowchart TD
  a["Incoming activity"] --> bot{"Member is a bot?"}
  bot -->|yes| none["organizationId = null"]
  bot -->|no| m{"Per-project affiliation<br/>active at timestamp?"}
  m -->|yes| use1["Use it"]
  m -->|no| d{"Username is an email on a<br/>non-excluded domain, and a work<br/>experience's org owns that domain?"}
  d -->|yes| pick["decidePrimaryOrganizationId"]
  d -->|no| t{"Work history active<br/>at timestamp?"}
  t -->|yes| uni["Drop universities if a<br/>company also matches"] --> pick
  t -->|no| u{"Most recent undated<br/>work experiences?"}
  u -->|yes| pick
  u -->|no| any{"Any undated<br/>work experience?"}
  any -->|yes| pick
  any -->|no| nul["organizationId = null"]
  pick --> out["activityRelations.organizationId<br/>and Kafka activities topic"]
  use1 --> out
```

`decidePrimaryOrganizationId` takes a single candidate as is; otherwise it prefers the one marked
primary, then the best `source` tier, then the organization with strictly more members, then the
longest date range.

## How a recalculation builds the timeline

`prepareMemberOrganizationAffiliationTimeline` produces three kinds of timeline items, each
limited to a disjoint slice of the member's activities.

```mermaid
flowchart TD
  start["Member"] --> load["Load work history<br/>(allowAffiliation not false,<br/>no Investor, Mentor or<br/>Board Member titles)"]
  start --> loadm["Load per-project<br/>affiliations"]
  load --> fb["Fallback org: undated primary,<br/>else earliest-created undated"]
  load --> base["Base timeline: walk day by day from<br/>earliest start to today, pick one org<br/>per day, fill gaps with fallback"]
  fb --> base
  load --> dom["Email-domain items: one org per<br/>verified primary domain of a<br/>work-history organization"]
  loadm --> man["Manual items: one timeline<br/>per project"]

  base --> s1["Activities without an email on<br/>those domains, not covered by an active<br/>manual project affiliation"]
  dom --> s2["Activities whose username<br/>is on that domain"]
  man --> s3["Activities in that project<br/>within the manual dates"]

  s1 --> w[("activityRelations")]
  s2 --> w
  s3 --> w
```

When several candidates are active on the same day, the recalculation picks one like this:

```mermaid
flowchart TD
  c["Candidates active on a day"] --> one{"Only one?"}
  one -->|yes| r1["Use it"]
  one -->|no| man{"Any per-project<br/>affiliation?"}
  man -->|yes| r2["Longest date range<br/>among them"]
  man -->|no| uni["Drop universities if<br/>a company also matches"]
  uni --> pri{"Any marked primary?"}
  pri -->|yes| r3["First primary with a start date,<br/>else first primary"]
  pri -->|no| dated{"How many have<br/>a start date?"}
  dated -->|one| r4["Use it"]
  dated -->|several| src["Keep only the dated ones,<br/>then the best source tier:<br/>ui, project-registry,<br/>email-domain, enrichment-*"]
  dated -->|none| cnt
  src --> cnt{"One org has strictly<br/>more members?"}
  cnt -->|yes| r5["Use it"]
  cnt -->|no| r6["Longest date range"]
```

## How the three paths differ

The three code paths share most of their selection order but not all of it:

| Step                                  | Ingest (`findAffiliation`) | Recalculation             | Public API                                |
| ------------------------------------- | -------------------------- | ------------------------- | ----------------------------------------- |
| Per-project affiliation wins          | Yes                        | Yes                       | Yes                                       |
| Email domain decides the organization | Yes                        | Yes                       | No                                        |
| Universities dropped for a company    | Date-matched step only     | Yes                       | No                                        |
| Member count for tie-break            | `organizationSegmentsAgg`  | `organizationSegmentsAgg` | Distinct members in `memberOrganizations` |
