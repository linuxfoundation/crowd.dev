# ADR-0033: Salesforce account slugs for Insights org pages

**Date**: 2026-10-01
**Status**: proposed
**Deciders**: Joana Maia

## Context

Insights org pages live at `/organization/<slug>`. Slugs were generated from the org name because neither CDP nor Insights had an org slug to use, and they were always meant to switch to the Salesforce slug once Salesforce data was integrated. The slug exists only in Tinybird: `organizations_populate_slug.pipe` rebuilds it every hour from `displayName`, and when two orgs share a name the one with more contributions gets the bare slug. A rename or a shift in contribution counts changes the slug, and the old URL returns 404 because nothing records past slugs. Salesforce accounts already have a slug (`Account.Slug__c`, `account_slug` in lf-dbt) that Org Lens uses in `/org/<slug>`. It does not change on rename. With `organizationAccounts` (Open Source Stacks, C1) CDP knows which orgs match a Salesforce account.

## Decision

For a CDP org matched to a Salesforce account with a valid slug, the Insights org slug is the Salesforce slug. Other orgs keep the name-based slug, and a name-based slug that collides with a Salesforce slug gets the `--N` suffix. Every slug an org has held is kept in Tinybird, and Insights answers an old slug with a 301 to the current one.

## Alternatives Considered

### Alternative 1: Keep name-based slugs for every org

- **Pros**: no change, no URL moves.
- **Cons**: slugs keep changing on rename and on contribution shifts; Insights and Org Lens use different addresses for the same company.
- **Why not**: the instability already produces 404s, and it leaves Insights with a second slug for the same company.

### Alternative 2: Switch to Salesforce slugs without redirect history

- **Pros**: smaller change; no new datasource or middleware.
- **Cons**: every matched org's current URL breaks at once, including search engine results and links shared outside Insights.
- **Why not**: the breakage is large and permanent, and the history also fixes the existing rename 404s.

### Alternative 3: Store the slug in Postgres `organizations`

- **Pros**: one stable value per org, readable by every service.
- **Cons**: collision handling and the hourly contribution ranking move into a write path; the merge and enrichment flows would all have to maintain it.
- **Why not**: slugs are only used by Insights, which already reads them from Tinybird; keeping them there limits the change to one pipe.

## Consequences

### Positive

- Matched orgs get a slug that survives renames and contribution shifts.
- Insights and Org Lens use the same slug for matched orgs, so links between them can reuse it.
- Old org URLs redirect instead of returning 404, for renames as well as this switch.

### Negative

- About every matched org's URL changes once; the old URL becomes a redirect.
- New lf-dbt column on `_SILVER_DIM_CDEV_ORG_TO_ACCOUNT`, so the dbt model is no longer untouched by Open Source Stacks.
- New Tinybird datasource, pipe and Insights middleware to maintain.

### Risks

- Salesforce does not enforce slug uniqueness. Two matched accounts with the same slug are ranked by contributions and the second gets `--N`, so neither page is lost.
- A Salesforce slug that is SFID-shaped or has characters outside `[a-z0-9-]` would produce a URL Insights can't route reliably. Such slugs fall back to the name-based slug.
- Switching before the history is seeded breaks URLs for a release. The history datasource and the redirect ship first, seeded with today's slugs, and the slug rule switches after.
- A slug that used to belong to one org can later become another org's current slug. The lookup checks current slugs first, so the current owner wins.
