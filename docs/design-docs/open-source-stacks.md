# Open Source Stacks: architecture

**Date**: 2026-09-30
**Status**: proposed
**Owner**: Joana Maia
**Product spec**: [Open Source Stacks Design Spec (14 Sep 2026)](../product-specs/open-source-stacks.md)
**Related ADRs**: [ADR-0032: Explicit `collections.type` discriminator](../adr/0032-collection-type-discriminator.md), [ADR-0033: Salesforce account slugs for Insights org pages](../adr/0033-salesforce-org-slugs.md), [ADR-0034: Salesforce data from Snowflake reaches Insights through CDP](../adr/0034-snowflake-salesforce-data-through-cdp.md)
**Prior approved decisions**: [pcc_sync_worker pattern for Snowflake → CDP syncs](https://docs.google.com/document/d/1t6HyZdHGM9TA47fyJ5jRQ96z4O5esmnCDu03yNS4lwI/edit?tab=t.0#heading=h.n2bz17vu9svn)

## 1. Summary

An Open Source stack is a public collection owned by a company. A company admin builds it as a workspace in Org Lens (LFX Self-Serve) and publishes it to LFX Insights. Insights shows it on the Collections tab, on the company's org page, and on the page of every project in it.

To an Insights reader a stack is a collection. Technically, a workspace and a collection are two entities in two systems:

- A **workspace** is a named list of projects for a company. It lives in lfx-v2-member-service and is private to the company's Org Lens users.
- A **collection** is a public list of projects on Insights. It lives in CDP Postgres.

A workspace becomes a collection only when someone publishes it. Publishing creates a `collections` row with `type = 'stack'`, linked to the workspace by its ID. Unpublishing soft-deletes that row. Unpublished workspaces never reach CDP or Insights.

Three systems are involved:

- **Self-Serve** is where people create, edit and publish workspaces.
- **CDP** stores the published ones as collections.
- **Insights** displays them.

```mermaid
flowchart LR
  subgraph SelfServe["LFX Self-Serve"]
    UI[Org Lens Projects page]
    BFF[Express BFF]
  end
  subgraph LFXv2["LFX v2"]
    MS[(member-service<br/>workspaces)]
    AC[access-check]
    QS[query-service]
  end
  subgraph CDP["CDP (crowd.dev)"]
    API[Public API]
    PG[(Postgres)]
    SYNC[Salesforce sync worker]
  end
  SF[(Snowflake)]
  TB[(Tinybird)]
  INS[LFX Insights]

  UI --> BFF
  BFF -- workspace edits --> MS
  BFF -- checks user access --> AC
  BFF -- publish, update, unpublish --> API
  API --> PG
  SF --> SYNC --> PG
  PG -- Sequin CDC --> TB
  INS -- collection pages --> PG
  INS -- org page and project page --> TB
  INS -- checks user access --> AC
  INS -- lists user's companies --> QS
  INS -- Create and Manage links --> UI
```

### Data model mapping

Where each table comes from, where it lands, and which Insights surface reads it. Tinybird datasources that copy a Postgres table keep the table's name.

```mermaid
flowchart LR
  subgraph SNOW["Snowflake (lf-dbt)"]
    S1[bronze_fivetran_salesforce_accounts]
    S2[_SILVER_DIM_CDEV_ORG_TO_ACCOUNT]
  end
  subgraph NATS["member-service (NATS KV)"]
    N1[org-workspaces]
    N2[org_workspace_projects]
  end
  BFF[Self-Serve BFF<br/>settings dialog:<br/>collection name, description]
  subgraph PG["CDP Postgres"]
    P1[organizationAccounts]
    P2[organizations]
    P3[collections<br/>type = stack]
    P4[collectionProjectSlugs]
    P5[collectionsInsightsProjects]
    P6[segments]
    P7[insightsProjects]
  end
  subgraph TBDS["Tinybird datasources"]
    T1[organizationAccounts]
    T2[organizations]
    T3[collections]
    T4[collectionsInsightsProjects]
    T5[insights_projects_populated_ds]
    T6[organizations_populated_slug]
    T7[organization_slug_history]
  end
  subgraph PIPES["Tinybird pipes"]
    X1[organizations_populate_slug]
    X2[org_page_profile]
    X3[org_page_stacks]
    X4[project_stack_organizations]
  end
  subgraph UIS["Insights"]
    U1[Org page]
    U2[Project page]
    U3[Collection page and Collections tab]
  end

  S1 -- account_slug --> S2
  S2 -- sync worker --> P1
  BFF -- 1. write workspace --> N1
  BFF -- 1. write projects --> N2
  N2 -- 2. stored project list --> BFF
  BFF -- 3. PUT snapshot --> P3
  BFF -- 3. PUT snapshot --> P4
  P4 -- slug to segment --> P6
  P6 -- segmentId --> P7
  P4 -. matched projects .-> P5
  P7 --> P5

  P1 -- Sequin --> T1
  P2 -- Sequin --> T2
  P3 -- Sequin --> T3
  P5 -- Sequin --> T4
  P7 -- Sequin, then copy pipe --> T5

  T1 --> X1
  T2 --> X1
  X1 --> T6
  X1 --> T7
  T6 --> X2
  T1 --> X2
  T2 --> X2
  X2 --> U1

  T6 --> X3
  T1 --> X3
  T3 --> X3
  T4 --> X3
  T5 --> X3
  X3 --> U1

  T5 --> X4
  T4 --> X4
  T3 --> X4
  T1 --> X4
  T2 --> X4
  T6 --> X4
  X4 --> U2

  P3 --> U3
  P1 --> U3
  P2 --> U3
  P5 --> U3
```

- `organizationAccounts` is the only new source of Salesforce data. Every Insights surface that shows a company joins it on the CDP org ID.
- Stack rows and their project links are written only by the CDP public API. The Self-Serve BFF calls it after member-service confirms the workspace write, and sends the project list member-service returned plus the collection name and description from the settings dialog. Nothing reads NATS KV directly.
- The collection page and the Collections tab read Postgres directly. The org page and project page read Tinybird.

## 2. How each system identifies a company

| System            | Company ID                                                                 |
| ----------------- | -------------------------------------------------------------------------- |
| Org Lens, LFX v2  | Salesforce account ID (SFID)                                               |
| CDP               | `organizations.id` (UUID)                                                  |
| Insights org page | Org slug, which resolves to CDP ID. Salesforce slug for matched orgs (C10) |

CDP has no SFID today. The warehouse already matches CDP orgs to Salesforce accounts in `_SILVER_DIM_CDEV_ORG_TO_ACCOUNT` (lf-dbt), which Org Lens uses. That model matches by domain first, then by name, then by a manually maintained Google Sheet.

## 3. Constraints and decisions

### C1: Mapping Org Lens companies to CDP organizations

**Problem.** A stack belongs to a Salesforce account, but Insights pages are keyed by CDP org. CDP can't translate one into the other.

**Decision.** Copy the warehouse mapping into a new CDP table, `organizationAccounts`. It has one row for every CDP org that the warehouse matches to a Salesforce account:

| Column           | Content                                                  |
| ---------------- | -------------------------------------------------------- |
| `accountId`      | SFID, primary key                                        |
| `organizationId` | CDP org ID, unique                                       |
| `accountName`    | Salesforce account name                                  |
| `accountLogoUrl` | Salesforce logo, nullable                                |
| `accountSlug`    | Salesforce slug, lowercased, nullable (C10)              |
| `matchMethod`    | `domain`, `name`, or `manual`, copied from the dbt model |
| `syncedAt`       | Last sync time                                           |

- A scheduled job loads the table from Snowflake, following the [approved `pcc_sync_worker` pattern](https://docs.google.com/document/d/1t6HyZdHGM9TA47fyJ5jRQ96z4O5esmnCDu03yNS4lwI/edit?tab=t.0#heading=h.n2bz17vu9svn) (export to S3, then a consumer upserts). Insights gets Salesforce data only through CDP, not through a Snowflake connector in Tinybird ([ADR-0034](../adr/0034-snowflake-salesforce-data-through-cdp.md)). The dbt model gains one output column, `account_slug`, for C10.
- Each run upserts every pair the model returns. An org that is added to CDP later, or a Salesforce account that gets matched later, gets its row on the first run after the warehouse matches it. A pair the model stops returning is deleted.
- The table holds only Salesforce-side values. It does not copy CDP org fields such as `displayName` or `logo`. The display name and logo are worked out when reading: `organizations` left joined to `organizationAccounts`, taking the Salesforce value when there is one and the CDP value otherwise.
- Insights uses that joined name and logo wherever it shows a company: stack cards, stack owner attribution, and the org page header. It joins on the CDP org ID it already has.
- When CDP merges two orgs, the merge workflow updates `organizationAccounts.organizationId` right away instead of waiting for the next warehouse run.
- The same table could replace domain and name guessing in the LFX memberships import (`backend/src/bin/scripts/import-lfx-memberships.ts`). **Automating that import is out of scope for this project (C11).**
- The table is replicated to Tinybird through Sequin so the new pipes can join on it.

**Stacks from companies with no CDP match.** A stack always stores its owner's SFID (`ownerAccountId`) plus the owner's name and logo (`ownerName`, `ownerLogoUrl`). Self-Serve sends those from Org Lens on every publish and update. The collection page shows that name as the owner. The company has no org page on Insights, so there is no link and the stack doesn't appear on project pages. Once the mapping sync adds the company, the join resolves its CDP org: the stack appears on the org page and project pages, and the owner name switches to the joined value. Nothing on the stack row needs to change.

### C2: Who owns a stack, and where that is checked

**Problem.** Stacks are only created and edited in Self-Serve, but Insights shows owner-only buttons (Create, Manage, owner empty states). The spec says these must never appear when clicking would fail (§5). Insights doesn't know which company a signed-in user belongs to, and today the org page sends everyone to Org Lens, including people who can't open it.

**Decision.**

- A user owns a company's stacks if they have `writer` on that company's `b2b_org` in LFX v2. In the FGA model `writer` also includes `owner` and `global_org_admin`. Viewers (`auditor`) can see the workspace in Org Lens but can't publish.
- Self-Serve enforces this before it writes anything. Workspace writes to member-service are checked by the LFX v2 gateway (Heimdall rule `b2b_org#writer`). Before calling CDP, the BFF checks `writer` itself with the same access-check call Insights uses below.
- Insights reads the same permission only to decide which buttons to show. A link from Insights grants nothing, since Self-Serve checks again when the user arrives.
- The Create button in the Collections tab and My collections empty states shows when the user is a writer on at least one company. Manage on a collection page, and the create prompt and "Manage Open Source stacks" link on an org page, show when the user is a writer on that company.
- The Insights server calls LFX v2 through its API gateway with the user's own token. It uses the same two services Self-Serve uses:
  - **One company** (collection page, org page): lfx-v2-access-check, `POST /access-check` with `{ "requests": ["b2b_org:<SFID>#writer"] }`. Each response line ends in `true` or `false`. Self-Serve's client is `apps/lfx-one/src/server/services/access-check.service.ts`.
  - **Any company** (Collections tab, My collections): there is no single LFX v2 endpoint. Self-Serve builds the list in `apps/lfx-one/src/server/services/org-role-grants.service.ts` from lfx-v2-query-service: `GET /query/resources?type=b2b_org_settings&tags=member:<username>` gives the companies the user was granted `writer` or `auditor` on directly, then `tags=parent_b2b_org_uid:<SFID>` finds subsidiaries that inherit the grant, and a batched `/access-check` confirms them. Insights repeats the first and last steps. It only needs to know whether the list of writer companies is empty.
  - Insights caches the answers for the session and hides the buttons if LFX v2 is down.
- The roster in `b2b_org_settings` lists only users granted a role in Org Lens. An `owner` or `global_org_admin` without a listed grant passes the single-company check but not the "any company" check, so they would see Manage but not the Create button in the Collections tab. Self-Serve has the same gap today.
- The spec's "verified employee by email domain" (§1.2) becomes "has writer access in Org Lens". Product needs to agree to that wording.
- We also fix the existing org page. Its Org Lens links (`header.vue`, `locked-contributors-section.vue`) show only to users with at least `auditor` on that company, and they open that company's page (`/org/<slug or SFID>/projects`) instead of a generic one.

### C3: Collections owned by a company

**Problem.** Today a collection is "curated" whenever `ssoUserId` is empty. A company-owned stack has no `ssoUserId`, so every curated list would pick it up.

**Decision** (details in [ADR-0032](../adr/0032-collection-type-discriminator.md)):

- Add `collections.type` with values `curated`, `community`, `stack`, filled from `ssoUserId` for existing rows.
- Every curated filter in Tinybird and Insights switches to `type = 'curated'`. This ships before any stack is written.
- New columns for stacks: `workspaceId` (member-service workspace UID), `ownerAccountId`, `ownerName`, `ownerLogoUrl`, `publishedAt`, `createdByUsername`, `updatedByUsername`. `ownerName` and `ownerLogoUrl` are used only while the owner has no CDP match (see C1).
- `workspaceId` is unique among stacks, so a workspace has at most one collection.
- Stacks are always `isPrivate = false`. An unpublished stack is soft-deleted (`deletedAt` set).
- The collection name and description are entered in the workspace settings dialog when publishing (spec §7.3). They are stored only on the collection. The workspace keeps its own name in member-service.

### C4: Where workspaces live, and how published ones reach CDP

**Problem.** Workspaces live in lfx-v2-member-service, in two NATS KV buckets:

- `org-workspaces`, one key per company (`org-workspaces.<SFID>`), holding every workspace's UID, name, and audit fields.
- `org_workspace_projects`, one key per workspace (`org_workspace_projects.<workspace UID>`), holding its projects as `project_slug` and optional `project_name`.

member-service has no read API. It publishes `lfx.index.org_workspace` and `lfx.index.org_workspace_project` events, the indexer writes them to OpenSearch, and Self-Serve reads them back through query-service. Those indexed documents are private (`Public: false`, readable with `auditor` on the company). There is no public or published field.

**Options.**

1. **Insights reads workspaces from LFX v2.** Not feasible. Query-service only returns workspaces to users with `auditor` on the company, so anonymous visitors could not see a stack. Tinybird can't read NATS or OpenSearch, so org and project pages would have to call LFX v2 per request, and the project page would need a cross-company search by project slug that LFX v2 doesn't offer.
2. **Replicate every workspace from NATS KV into CDP.** CDP would need its first NATS consumer, and the events are fire-and-forget, so a reconciliation job would be needed too. It copies private workspaces that Insights never shows.
3. **Copy a workspace into CDP only when it is published.** Recommended.

**Decision.** Workspaces stay in member-service, unchanged. When a writer turns on the public toggle and saves, the Self-Serve BFF sends CDP a full snapshot of the workspace: collection name, description, project slugs and names, owner name and logo, and the user's LFID. CDP creates or updates the collection for that `workspaceId`. When the workspace is edited while published, the BFF writes to member-service first and then sends the new snapshot to CDP. The project list in the snapshot is the one member-service returns after the write, not what the page shows, so CDP only receives projects member-service has stored. Unpublishing, or deleting a published workspace, soft-deletes the collection. Republishing restores the same row, so the collection keeps its URL.

CDP is the record of whether a workspace is published. Self-Serve reads the published stacks for a company from CDP to show the Published tag and URL. member-service needs no change.

The API is part of the crowd.dev public API. It uses Auth0 machine-to-machine tokens and Zod validation (`validateOrThrow`). Every route includes the company's SFID:

| Method | Path                                     | Purpose                              | Scope              |
| ------ | ---------------------------------------- | ------------------------------------ | ------------------ |
| GET    | `/v1/org-stacks/:accountId`              | Published stacks for a company       | `read:org-stacks`  |
| PUT    | `/v1/org-stacks/:accountId/:workspaceId` | Publish, or update a published stack | `write:org-stacks` |
| DELETE | `/v1/org-stacks/:accountId/:workspaceId` | Unpublish (soft delete)              | `write:org-stacks` |

- **Permissions.** The Self-Serve server checks `writer` in LFX v2 before it calls CDP and passes the user's LFID for the audit columns. CDP trusts Self-Serve the way it already trusts callers of `write:organizations`.
- **PUT is idempotent.** It replaces the stack's name, description and project list with the snapshot. A failed call can be retried with the same body, and a retry after a later edit can't leave old projects behind.
- **Drift.** If the member-service write succeeds and the CDP call fails, the published stack is behind the workspace until the next save. The BFF retries, and if that fails it tells the user the public page wasn't updated.
- **Getting data to Insights.** Sequin already replicates `collections` and `collectionsInsightsProjects` to Tinybird. The new tables are added to the Sequin publication.
- **New runtime dependency.** Publishing and editing a published workspace fail if the CDP API is down. Private workspaces don't depend on CDP.

### C5: Matching workspace projects to Insights projects

**Problem.** Workspaces list projects by LF project slug. Some of those projects aren't on Insights at all.

**Decision.**

- New table `collectionProjectSlugs(collectionId, projectSlug, projectName, addedBy, createdAt)` holds every project in the published workspace, exactly as Self-Serve sent it.
- CDP fills `collectionsInsightsProjects` for the slugs it can match. It looks up `segments.slug` by the LF slug, then finds the Insights project with that `segmentId`. Project segments created through CDP use the LF slug as `segments.slug`, so LF projects match out of the box. Matching on `segmentId` doesn't depend on `insightsProjects.slug`.
- **Bug to fix.** When `pcc_sync_worker` creates an Insights project (`pccProjectConsumer.ts`), it sets `slug = generate_slug('insightsProjects', name)`, so the slug comes from the project name. It should use the PCC project slug, like Insights projects created through CDP. Matching in this project works either way, but those Insights project URLs differ from the LF slug.
- Projects that don't match stay in `collectionProjectSlugs` and don't appear on Insights. Self-Serve shows "N projects not yet on Insights".
- When a project is added to Insights, CDP links it to any stacks that already list it. This runs either as a hook in `pcc_sync_worker` or as a small cron job; we pick one during implementation.
- **Known gap.** `segments.slug` doesn't change when a project's LF slug changes (`pccProjectConsumer.ts` only logs the drift). Projects with a changed slug stay unmatched until that sync is automated.

### C6: Links between Insights and Self-Serve

**Problem.** People start on Insights but create and edit stacks in Self-Serve. Self-Serve has no link that opens the settings dialog directly, and no way to send the user back to Insights.

**Decision.**

- Self-Serve keeps its company switcher. Each page shows one company.
- **Create from the Collections tab or My collections** opens `/org/projects?action=new-workspace&returnTo=<Insights URL>` for the company already selected in Self-Serve.
- **Create from an org page** opens `/org/<slug or SFID>/projects?action=new-workspace&returnTo=…`.
- **Manage on a collection page**, which replaces the like button for owners, opens `/org/<slug or SFID>/projects?workspace=<workspaceId>&settings=open&returnTo=…`.
- **"Manage Open Source stacks" on an org page**, below the stack list, opens `/org/<slug or SFID>/projects?returnTo=…`.
- Self-Serve changes:
  - Support the `action` and `settings` parameters.
  - Add a public toggle, collection name, and description to the settings dialog. Both fields are required when the toggle is on.
  - Show published workspaces with a tag, their URL, and a menu.
  - Ask for confirmation before unpublishing.
  - Point the LF logo back to `returnTo`, allowing only the Insights host.

### C7: Project page, "Organizations with this project in their Open Source stacks"

**How Insights reads Tinybird today.** Sequin copies Postgres tables into Tinybird datasources with the same names (`collections`, `collectionsInsightsProjects`, `organizations`). Each copy keeps every version of a row, so readers add `FINAL` to get the latest one. A pipe is a named SQL query over datasources that Tinybird serves as an HTTP endpoint. The Insights server calls pipes by name (`/v0/pipes/<name>.json`) from its API handlers, passing URL values such as the project slug as parameters. The project page already passes its slug to pipes that filter `insights_projects_populated_ds`, Tinybird's table of Insights projects.

**Decision.** New pipe `project_stack_organizations`, called from a new Insights handler with the project slug. It:

1. Finds the project's ID in `insights_projects_populated_ds`.
2. Finds the stacks that include it in `collectionsInsightsProjects`, keeping only `collections` rows with `type = 'stack'` and no `deletedAt`.
3. Turns each stack's `ownerAccountId` into a CDP org through `organizationAccounts`, and drops stacks whose owner has no match, because they have no org page to link to.
4. Returns one row per company with the joined name and logo (C1), `headline` as the description, `employees`, and the org slug from `organizations_populated_slug` for the link.

The section sits in the left column of the project overview, below the health scorecard.

### C8: Several stacks on one org page

**How the org page reads Tinybird today.** The org page URL holds the org slug. Each section calls its own pipe (`org_page_profile`, `org_page_kpis`, `org_page_projects`, …). Each pipe first turns the slug into the CDP org ID through `organizations_populated_slug`, a datasource rebuilt every hour by `organizations_populate_slug` (C10).

**Decision.** New pipe `org_page_stacks`, called with the org slug. It:

1. Turns the slug into the CDP org ID through `organizations_populated_slug`.
2. Finds the company's SFID in `organizationAccounts`.
3. Lists the `collections` rows with that `ownerAccountId`, `type = 'stack'` and no `deletedAt`.
4. Returns name, slug, description, project count and the first few project logos (from `collectionsInsightsProjects` and `insights_projects_populated_ds`), newest first by `greatest(publishedAt, updatedAt)`.

### C9: Where Insights reads stacks from

**Decision.**

- The collection page, the Collections tab, and search read from Postgres, as collections do today (`communityCollection.repo.ts`), filtered to `type = 'stack'` and `deletedAt IS NULL`. An unpublished stack returns 404 immediately.
- The org page list (C8) and the project page list (C7) read from Tinybird.
- Tinybird updates lag behind Postgres. After an unpublish, an org or project page can link to the removed stack for a short time. We accept this.
- The Tinybird `collections` datasource needs `type`, `workspaceId`, `ownerAccountId`, `ownerName`, `ownerLogoUrl`, `deletedAt`, and `publishedAt`.

### C10: Org page slugs

**Context.** Insights org slugs were generated from the org name because neither CDP nor Insights had an org slug to use. They were always meant to switch to the Salesforce slug once Salesforce data was integrated.

**Problem.** Insights org slugs exist only in Tinybird. `organizations_populate_slug.pipe` rebuilds them every hour from `displayName`, so a rename changes the slug, and two orgs with the same name can swap slugs as their contribution counts change. Old slugs return 404. Org Lens uses a different address for the same company, the Salesforce slug.

**Decision** (details in [ADR-0033](../adr/0033-salesforce-org-slugs.md)):

- A matched org whose `organizationAccounts.accountSlug` is a valid slug (`[a-z0-9-]`, not SFID-shaped) uses it. Every other org keeps the name-based slug.
- Salesforce slugs win collisions. A name-based slug that clashes with one gets `--N`. If two matched accounts share a Salesforce slug, the one with more contributions keeps it.
- New Tinybird datasource `organization_slug_history (slug, organizationId, lastSeenAt)`, seeded with today's slugs and appended by each hourly run.
- An Insights org URL that isn't a current slug is looked up in the history and answered with a 301 to the org's current slug. Current slugs are checked first, so a reused slug goes to its current owner.
- Rollout: history and redirect first, then the slug rule.
- For matched orgs the Insights slug equals the Org Lens slug, so the Org Lens links in C2 and C6 reuse it.

### C11: LF memberships from Snowflake

**Out of scope for this project.** Open Source Stacks ships without it. It is recorded here because it reuses the sync worker and `organizationAccounts` from C1, and should be built on top of them afterwards.

**Problem.** `lfxMemberships` is loaded by hand from a CSV (`import-lfx-memberships.ts`) and matched to CDP orgs by domain and name. It has no SFID and is unique on `accountName`. Org pages read it for the membership badge and the Org Lens redirect.

**Decision.**

- The Salesforce sync worker (C1, [ADR-0034](../adr/0034-snowflake-salesforce-data-through-cdp.md)) gains a second export, `silver_dim_memberships`, keyed by account SFID and project SFID.
- It writes to a new table, `organizationMemberships`, keyed by `accountId`, project SFID and membership term. It stores no `organizationId`: readers join `organizationAccounts`, as stacks do. Memberships of unmatched companies are kept and appear once the mapping exists, and org merges only touch `organizationAccounts`.
- The project SFID matches `segments.sourceId`.
- `lfxMemberships` keeps serving until the two agree per project. Then `org_page_profile` and the other readers switch, and `lfxMemberships` and the CSV script are removed.

## 4. What happens when

### Publishing

1. On Insights, the owner clicks Create or Manage and lands in Self-Serve.
2. They turn on the public toggle, enter a collection name and description, and save.
3. Self-Serve checks they are a writer for that company, then calls `PUT /v1/org-stacks/:accountId/:workspaceId` with the workspace snapshot.
4. CDP creates the collection (or restores it, if it was published before), sets `publishedAt = now()`, writes `collectionProjectSlugs` and links the matched projects in `collectionsInsightsProjects`.
5. The collection page is live straight away. The org page and project pages show it once Tinybird catches up.

### Editing a published workspace

1. Self-Serve saves the change to member-service, as it does today.
2. It then calls `PUT /v1/org-stacks/:accountId/:workspaceId` with the new snapshot.
3. CDP replaces the name, description and project list, and relinks `collectionsInsightsProjects`.
4. Tinybird picks up the change through Sequin.

Edits to an unpublished workspace go to member-service only.

### Unpublishing

1. The owner confirms unpublish in Self-Serve, which calls `DELETE /v1/org-stacks/:accountId/:workspaceId`.
2. CDP sets `deletedAt`. The collection URL returns 404 right away, and the cards disappear once Tinybird catches up. The workspace stays in Org Lens.

Deleting a published workspace in Self-Serve makes the same call.

## 5. Product impact

- **Company name, logo and slug come from Salesforce.** For every company matched to a Salesforce account, Insights shows the Salesforce name and logo on the org page, stack cards and project page cards, and uses the Salesforce slug in the org page URL. CDP values are only a fallback for unmatched companies. Insights, Org Lens and the other LFX products then show the same company the same way.
- **Org page URLs change once for matched companies.** The old URL redirects (C10).
- **Subsidiaries stay separate, and Insights shows no parent roll-up.** The mapping does not roll a subsidiary up to its parent. Red Hat's CDP org maps to the Red Hat account and IBM's to the IBM account, each with its own org page and its own stacks. IBM's org page does not include Red Hat's stacks, contributors or projects, and no page shows a parent and its subsidiaries combined. This is the expected behavior for now.
  - The domain match prefers a top-level account only when several Salesforce accounts share the same domain.
  - The mapping is one-to-one. If a parent's and a subsidiary's CDP orgs both match the same account, only the one with more members keeps it and the other stays unmatched.
  - Org Lens also shows subsidiaries as separate companies. A writer on IBM inherits writer on Red Hat, so they can publish stacks for either, and each stack shows the company it was published for.
  - Salesforce's parent roll-up (`_4_sf_account_to_parent_account` in lf-dbt) is used only for memberships, which matters for C11.

## 6. Dependencies on other teams

| What we need                                                         | From           | Blocks                     |
| -------------------------------------------------------------------- | -------------- | -------------------------- |
| LFX v2 accepts the Insights Auth0 token (audience or token exchange) | LFX platform   | All permission checks (C2) |
| Publish toggle, snapshot calls to CDP, and the hand-off parameters   | LFX Self-Serve | Publishing (C4, C6)        |
| "Verified employee" means "writer in Org Lens"                       | Product        | Copy for C2                |

## 7. Open questions

1. The intro dialog (spec §6) says "{Organization}". Which company should it name when a user opens Create from the Collections tab and is a writer on more than one? Product needs to give a fallback.
2. Should new Insights projects be linked to existing stacks by a `pcc_sync_worker` hook or a cron job? We decide during implementation.
3. What is the difference between a workspace and a collection, for the people using Self-Serve? This doc treats them as two entities joined at publish time (§1). Product needs to confirm that, and how the settings dialog explains it (spec §7.2).
4. Who in Self-Serve can create and publish a stack, and is there a dedicated role? Today any `writer` on the company can edit workspaces, which includes `owner` and `global_org_admin` in the FGA model. There is no stack-specific role.
5. Can a user add a non-LF project to a workspace? The Self-Serve project picker lists the LFX onboarded project catalog, but neither the BFF nor member-service validates the slugs, so an API caller can add any slug.
6. Do we accept a delay between publishing in Self-Serve and the stack showing up on Insights, or do we want it as close to immediate as possible? With this design the collection page is immediate and the org and project pages follow Sequin lag.
7. Are we fine with adding an LF project in Self-Serve that doesn't exist in CDP yet? With this design it is stored and hidden on Insights until CDP onboards it (C5).
