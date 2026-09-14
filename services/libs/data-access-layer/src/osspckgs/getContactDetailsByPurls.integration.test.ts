import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'

import { getContactDetailsByPurls } from './api'

// Integration test: hits the running packages-db. Skipped automatically when
// any of the DB env vars are missing so unit-test runs in CI stay green.
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'akrites-external-contact-fixture'
const repoUrl = `https://github.com/example/${FIXTURE_TAG}-repo`

describe.skipIf(!HAVE_DB)('getContactDetailsByPurls — real packages-db', () => {
  let qx: QueryExecutor

  const withContactsPurl = `pkg:test-fixture/${FIXTURE_TAG}/with-contacts`
  const noContactsPurl = `pkg:test-fixture/${FIXTURE_TAG}/no-contacts`
  const missingPurl = `pkg:test-fixture/${FIXTURE_TAG}/does-not-exist`

  async function cleanupFixtures(): Promise<void> {
    await qx.result(
      `DELETE FROM security_contacts WHERE repo_id IN (SELECT id FROM repos WHERE url = $(url))`,
      { url: repoUrl },
    )
    await qx.result(
      `DELETE FROM package_repos WHERE package_id IN (SELECT id FROM packages WHERE ingestion_source = $(tag))`,
      { tag: FIXTURE_TAG },
    )
    await qx.result(`DELETE FROM packages WHERE ingestion_source = $(tag)`, { tag: FIXTURE_TAG })
    await qx.result(`DELETE FROM repos WHERE url = $(url)`, { url: repoUrl })
  }

  beforeAll(async () => {
    const conn = await getDbConnection({
      host: process.env.CROWD_PACKAGES_DB_WRITE_HOST ?? '',
      port: parseInt(process.env.CROWD_PACKAGES_DB_PORT ?? '0', 10),
      database: process.env.CROWD_PACKAGES_DB_DATABASE ?? '',
      user: process.env.CROWD_PACKAGES_DB_USERNAME ?? '',
      password: process.env.CROWD_PACKAGES_DB_PASSWORD ?? '',
    })
    qx = pgpQx(conn)
    await cleanupFixtures()

    const withRow = await qx.selectOne(
      `INSERT INTO packages (purl, ecosystem, name, ingestion_source)
       VALUES ($(purl), 'npm', 'with-contacts-fixture', $(tag))
       RETURNING id`,
      { purl: withContactsPurl, tag: FIXTURE_TAG },
    )
    const withPackageId = withRow.id as string

    await qx.result(
      `INSERT INTO packages (purl, ecosystem, name, ingestion_source)
       VALUES ($(purl), 'npm', 'no-contacts-fixture', $(tag))`,
      { purl: noContactsPurl, tag: FIXTURE_TAG },
    )

    const repoRow = await qx.selectOne(
      `INSERT INTO repos (url, security_policy_url, pvr_enabled, vulnerability_reporting_url, bug_bounty_url)
       VALUES ($(url), 'https://example.org/SECURITY.md', true, 'https://example.org/report-vulnerability', 'https://example.org/bug-bounty')
       RETURNING id`,
      { url: repoUrl },
    )
    const repoId = repoRow.id as string

    await qx.result(
      `INSERT INTO package_repos (package_id, repo_id, source, confidence)
       VALUES ($(packageId), $(repoId), 'declared', 0.9)`,
      { packageId: withPackageId, repoId },
    )

    await qx.result(
      `INSERT INTO security_contacts (repo_id, channel, value, role, confidence, score)
       VALUES ($(repoId), 'github-pvr', 'https://example.org/advisories/new', 'security-team', 'PRIMARY', 0.94)`,
      { repoId },
    )
    await qx.result(
      `INSERT INTO security_contacts (repo_id, channel, value, role, confidence, score)
       VALUES ($(repoId), 'email', 'security@example.org', 'security-team', 'SECONDARY', 0.735)`,
      { repoId },
    )
  }, 30_000)

  afterAll(async () => {
    if (qx) await cleanupFixtures()
  })

  it('returns no row for a purl that does not resolve to a package', async () => {
    const rows = await getContactDetailsByPurls(qx, [missingPurl])
    expect(rows).toHaveLength(0)
  })

  it('returns a row with null securityContacts for a package with no linked repo/contacts', async () => {
    const [row] = await getContactDetailsByPurls(qx, [noContactsPurl])
    expect(row.purl).toBe(noContactsPurl)
    expect(row.securityContacts).toBeNull()
  })

  it('returns the security contacts (score-ordered) and repo policy fields when present', async () => {
    const [row] = await getContactDetailsByPurls(qx, [withContactsPurl])
    expect(row.securityPolicyUrl).toBe('https://example.org/SECURITY.md')
    expect(row.pvrEnabled).toBe(true)
    expect(row.vulnerabilityReportingUrl).toBe('https://example.org/report-vulnerability')
    expect(row.bugBountyUrl).toBe('https://example.org/bug-bounty')
    expect(row.securityContacts).not.toBeNull()
    expect(row.securityContacts).toHaveLength(2)
    // json_agg ORDER BY score DESC → PRIMARY (0.94) first.
    expect(row.securityContacts?.[0].confidence).toBe('PRIMARY')
  })

  it('batches many purls, omitting the misses', async () => {
    const rows = await getContactDetailsByPurls(qx, [missingPurl, noContactsPurl, withContactsPurl])
    const purls = new Set(rows.map((r) => r.purl))
    expect(purls.has(missingPurl)).toBe(false)
    expect(purls.has(noContactsPurl)).toBe(true)
    expect(purls.has(withContactsPurl)).toBe(true)
  })
})
