import { QueryExecutor, formatQuery } from '../queryExecutor'
import type { AffiliatedMemberIdentityRow } from './memberContributors'

export type IdentityType = 'github-login' | 'email'
export type AffiliationConfidence = 'cdp' | 'email_domain' | 'github_company'

export interface IdentityKey {
  identityType: IdentityType
  identityValue: string
}

export interface IdentityAffiliationResult {
  akritesMember: string | null
  cdpOrganizationId: string | null
  confidence: AffiliationConfidence | null
  confidenceScore: number | null
  cdpMemberId: string | null
  matchedDomain: string | null
  matchedAlias: string | null
}

export type StagingAffiliationRow = IdentityKey & IdentityAffiliationResult

export interface MergeCounts {
  inserted: number
  changed: number
  unchanged: number
  removed: number
  byMethod: Record<AffiliationConfidence | 'unresolved', number>
}

interface MergeGuardCounts {
  total: number
  unresolved: number
  matched: number
}

interface MethodCount {
  method: AffiliationConfidence | 'unresolved'
  n: number
}

const RESULT_COLUMNS = [
  'akrites_member',
  'cdp_organization_id',
  'confidence',
  'confidence_score',
  'cdp_member_id',
  'matched_domain',
  'matched_alias',
]

const SAME_KEY = 's.identity_type = a.identity_type AND s.identity_value = a.identity_value'

function prefixedResultColumns(alias: string): string {
  return RESULT_COLUMNS.map((column) => `${alias}.${column}`).join(', ')
}

export async function truncateIdentityAffiliationsStaging(qx: QueryExecutor): Promise<void> {
  await qx.selectNone(`TRUNCATE identity_affiliations_staging`)
}

export async function fillIdentityAffiliationsStaging(qx: QueryExecutor): Promise<number> {
  return qx.result(
    `
      INSERT INTO identity_affiliations_staging (identity_type, identity_value)
      SELECT DISTINCT identity_type, identity_value
      FROM repo_contributors
      WHERE identity_type IN ('github-login', 'email')
      UNION
      SELECT 'github-login', lower(trim(github_login))
      FROM maintainers
      WHERE nullif(trim(github_login), '') IS NOT NULL
      UNION
      SELECT 'email', lower(trim(email))
      FROM maintainers
      WHERE nullif(trim(email), '') IS NOT NULL
      ON CONFLICT DO NOTHING
    `,
  )
}

export async function readIdentityAffiliationsStagingPage(
  qx: QueryExecutor,
  afterKey: IdentityKey | null,
  limit: number,
): Promise<IdentityKey[]> {
  const afterKeyFilter = afterKey
    ? 'WHERE (identity_type, identity_value) > ($(identityType), $(identityValue))'
    : ''

  return qx.select(
    `
      SELECT identity_type AS "identityType", identity_value AS "identityValue"
      FROM identity_affiliations_staging
      ${afterKeyFilter}
      ORDER BY identity_type, identity_value
      LIMIT $(limit)
    `,
    { ...afterKey, limit },
  )
}

function formatStagingValuesRow(row: StagingAffiliationRow): string {
  return formatQuery(
    `($(identityType), $(identityValue), $(akritesMember), $(cdpOrganizationId)::uuid, $(confidence), $(confidenceScore)::numeric, $(cdpMemberId)::uuid, $(matchedDomain), $(matchedAlias))`,
    row,
  )
}

export async function writeIdentityAffiliationsStagingResults(
  qx: QueryExecutor,
  rows: StagingAffiliationRow[],
  resolvedAt: Date,
): Promise<number> {
  if (rows.length === 0) {
    return 0
  }

  return qx.result(
    `
      UPDATE identity_affiliations_staging AS s SET
        akrites_member = v.akrites_member,
        cdp_organization_id = v.cdp_organization_id,
        confidence = v.confidence,
        confidence_score = v.confidence_score,
        cdp_member_id = v.cdp_member_id,
        matched_domain = v.matched_domain,
        matched_alias = v.matched_alias,
        resolved_at = $(resolvedAt)
      FROM (VALUES $(rows:raw)) AS v(
        identity_type, identity_value, akrites_member, cdp_organization_id,
        confidence, confidence_score, cdp_member_id, matched_domain, matched_alias
      )
      WHERE s.identity_type = v.identity_type AND s.identity_value = v.identity_value
    `,
    { rows: rows.map(formatStagingValuesRow).join(', '), resolvedAt },
  )
}

export async function findIdentityKeysWithoutAffiliation(
  qx: QueryExecutor,
  keys: IdentityKey[],
): Promise<IdentityKey[]> {
  if (keys.length === 0) {
    return []
  }
  return qx.select(
    `
      SELECT k.identity_type AS "identityType", k.identity_value AS "identityValue"
      FROM unnest($(types)::text[], $(values)::text[]) AS k(identity_type, identity_value)
      WHERE NOT EXISTS (
        SELECT 1 FROM identity_affiliations a
        WHERE a.identity_type = k.identity_type AND a.identity_value = k.identity_value
      )
    `,
    {
      types: keys.map((key) => key.identityType),
      values: keys.map((key) => key.identityValue),
    },
  )
}

export async function findAffiliationsForIdentityKeys(
  qx: QueryExecutor,
  keys: IdentityKey[],
): Promise<AffiliatedMemberIdentityRow[]> {
  if (keys.length === 0) {
    return []
  }
  return qx.select(
    `
      SELECT COALESCE(a.cdp_member_id::text, a.identity_type || ':' || a.identity_value) AS person,
             'github'::text AS source, NULL::text AS role, 'contributor'::text AS "roleKind",
             a.identity_type AS "identityType", a.identity_value AS "identityValue",
             NULL::timestamptz AS "endedAt", NULL::timestamptz AS "lastSeenAt",
             NULL::text AS "registryUsername", NULL::text AS "registryDisplayName",
             a.akrites_member AS "akritesMember", a.cdp_organization_id::text AS "cdpOrganizationId",
             a.confidence, a.confidence_score AS "confidenceScore"
      FROM unnest($(types)::text[], $(values)::text[]) AS k(identity_type, identity_value)
      JOIN identity_affiliations a
        ON a.identity_type = k.identity_type AND a.identity_value = k.identity_value
      WHERE a.akrites_member IS NOT NULL
      ORDER BY person, a.identity_type, a.identity_value
    `,
    {
      types: keys.map((key) => key.identityType),
      values: keys.map((key) => key.identityValue),
    },
  )
}

export async function insertIdentityAffiliations(
  qx: QueryExecutor,
  rows: StagingAffiliationRow[],
  resolvedAt: Date,
): Promise<number> {
  if (rows.length === 0) {
    return 0
  }
  return qx.result(
    `
      INSERT INTO identity_affiliations (
        identity_type, identity_value, ${RESULT_COLUMNS.join(', ')}, resolved_at, changed_at
      )
      SELECT
        v.identity_type, v.identity_value, ${prefixedResultColumns('v')},
        $(resolvedAt), $(resolvedAt)
      FROM (VALUES $(rows:raw)) AS v(
        identity_type, identity_value, akrites_member, cdp_organization_id,
        confidence, confidence_score, cdp_member_id, matched_domain, matched_alias
      )
      ON CONFLICT (identity_type, identity_value) DO NOTHING
    `,
    { rows: rows.map(formatStagingValuesRow).join(', '), resolvedAt },
  )
}

async function assertStagingReadyToMerge(tx: QueryExecutor, runStartedAt: Date): Promise<void> {
  const { total, unresolved, matched }: MergeGuardCounts = await tx.selectOne(
    `
      SELECT
        count(*)::int AS total,
        count(*) FILTER (WHERE resolved_at IS NULL OR resolved_at <> $(runStartedAt))::int AS unresolved,
        count(*) FILTER (WHERE confidence IS NOT NULL)::int AS matched
      FROM identity_affiliations_staging
    `,
    { runStartedAt },
  )

  if (total === 0 || unresolved > 0 || matched === 0) {
    throw new Error(
      `Refusing to merge identity affiliations: total ${total}, unresolved ${unresolved}, matched ${matched}`,
    )
  }
}

async function deleteAffiliationsMissingFromStaging(
  tx: QueryExecutor,
  runStartedAt: Date,
): Promise<number> {
  return tx.result(
    `
      DELETE FROM identity_affiliations a
      WHERE NOT EXISTS (
        SELECT 1 FROM identity_affiliations_staging s
        WHERE ${SAME_KEY}
      )
      AND NOT EXISTS (
        SELECT 1 FROM repo_contributors rc
        WHERE rc.identity_type = a.identity_type AND rc.identity_value = a.identity_value
      )
      AND a.resolved_at < $(runStartedAt)
    `,
    { runStartedAt },
  )
}

async function updateChangedAffiliations(tx: QueryExecutor, runStartedAt: Date): Promise<number> {
  const assignments = RESULT_COLUMNS.map((column) => `${column} = s.${column}`).join(', ')

  return tx.result(
    `
      UPDATE identity_affiliations a SET
        ${assignments},
        resolved_at = $(runStartedAt),
        changed_at = $(runStartedAt),
        updated_at = NOW()
      FROM identity_affiliations_staging s
      WHERE ${SAME_KEY}
        AND (${prefixedResultColumns('a')}) IS DISTINCT FROM (${prefixedResultColumns('s')})
    `,
    { runStartedAt },
  )
}

async function touchUnchangedAffiliations(tx: QueryExecutor, runStartedAt: Date): Promise<number> {
  return tx.result(
    `
      UPDATE identity_affiliations a SET
        resolved_at = $(runStartedAt),
        updated_at = NOW()
      FROM identity_affiliations_staging s
      WHERE ${SAME_KEY}
        AND (${prefixedResultColumns('a')}) IS NOT DISTINCT FROM (${prefixedResultColumns('s')})
        AND a.resolved_at <> $(runStartedAt)
    `,
    { runStartedAt },
  )
}

async function insertNewAffiliations(tx: QueryExecutor, runStartedAt: Date): Promise<number> {
  return tx.result(
    `
      INSERT INTO identity_affiliations (
        identity_type, identity_value, ${RESULT_COLUMNS.join(', ')}, resolved_at, changed_at
      )
      SELECT
        s.identity_type, s.identity_value, ${prefixedResultColumns('s')},
        $(runStartedAt), $(runStartedAt)
      FROM identity_affiliations_staging s
      WHERE NOT EXISTS (
        SELECT 1 FROM identity_affiliations a
        WHERE ${SAME_KEY}
      )
      ON CONFLICT (identity_type, identity_value) DO NOTHING
    `,
    { runStartedAt },
  )
}

async function countStagingByMethod(tx: QueryExecutor): Promise<MergeCounts['byMethod']> {
  const rows: MethodCount[] = await tx.select(
    `
      SELECT coalesce(confidence, 'unresolved') AS method, count(*)::int AS n
      FROM identity_affiliations_staging
      GROUP BY 1
    `,
  )

  const byMethod: MergeCounts['byMethod'] = {
    cdp: 0,
    email_domain: 0,
    github_company: 0,
    unresolved: 0,
  }
  for (const { method, n } of rows) {
    byMethod[method] = n
  }
  return byMethod
}

export async function mergeIdentityAffiliationsStaging(
  qx: QueryExecutor,
  runStartedAt: Date,
): Promise<MergeCounts> {
  return qx.tx(async (tx) => {
    await assertStagingReadyToMerge(tx, runStartedAt)
    const removed = await deleteAffiliationsMissingFromStaging(tx, runStartedAt)
    const changed = await updateChangedAffiliations(tx, runStartedAt)
    const unchanged = await touchUnchangedAffiliations(tx, runStartedAt)
    const inserted = await insertNewAffiliations(tx, runStartedAt)
    const byMethod = await countStagingByMethod(tx)
    return { inserted, changed, unchanged, removed, byMethod }
  })
}
