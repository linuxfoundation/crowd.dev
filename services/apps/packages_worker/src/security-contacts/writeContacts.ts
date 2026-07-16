import { QueryExecutor, formatQuery } from '@crowd/data-access-layer/src/queryExecutor'
import { prepareBulkInsert } from '@crowd/data-access-layer/src/utils'
import { getServiceChildLogger } from '@crowd/logging'

import { ProcessRepoResult, RepoPolicies, ScoredContact } from './types'

const log = getServiceChildLogger('security-contacts')

const CONTACT_COLUMNS = [
  'repo_id',
  'channel',
  'value',
  'role',
  'name',
  'score',
  'confidence',
  'provenance',
  'reachable',
  'reachability_reason',
]

const CONTACT_UPSERT_SET = `(repo_id, channel, value) DO UPDATE SET
     role = EXCLUDED.role,
     name = EXCLUDED.name,
     score = EXCLUDED.score,
     confidence = EXCLUDED.confidence,
     provenance = EXCLUDED.provenance,
     reachable = EXCLUDED.reachable,
     reachability_reason = EXCLUDED.reachability_reason,
     last_refreshed = NOW(),
     updated_at = NOW(),
     deleted_at = NULL`

function toContactRow(repoId: string, c: ScoredContact) {
  return {
    repo_id: repoId,
    channel: c.channel,
    value: c.value,
    role: c.role,
    name: c.name ?? null,
    score: c.score,
    confidence: c.confidence,
    provenance: JSON.stringify(c.provenance),
    reachable: c.reachable,
    reachability_reason: c.reachabilityReason,
  }
}

// Advances contacts_last_refreshed only, so a failed pass isn't reprocessed this sweep.
export async function markRepoAttempted(qx: QueryExecutor, repoId: string): Promise<void> {
  await qx.result('UPDATE repos SET contacts_last_refreshed = NOW() WHERE id = $(repoId)', {
    repoId,
  })
}

// Assumes at most one writer per repoId at a time (see processBatch.ts/schedule.ts) — no locking.
//
// Soft-delete: mark every active row stale, then upsert this pass's contacts, reviving whatever
// was rediscovered. Readers of this table must filter on deleted_at IS NULL.
export async function writeContacts(
  qx: QueryExecutor,
  repoId: string,
  contacts: ScoredContact[],
  policies: Partial<RepoPolicies>,
): Promise<void> {
  await qx.tx(async (tx) => {
    await tx.result(
      'UPDATE security_contacts SET deleted_at = NOW(), updated_at = NOW() WHERE repo_id = $(repoId) AND deleted_at IS NULL',
      { repoId },
    )

    if (contacts.length > 0) {
      await tx.result(
        prepareBulkInsert(
          'security_contacts',
          CONTACT_COLUMNS,
          contacts.map((c) => toContactRow(repoId, c)),
          CONTACT_UPSERT_SET,
        ),
      )
    }

    await tx.result(
      // vulnerability_reporting_url is overwritten only once PVR is authoritatively resolved.
      `UPDATE repos SET
         security_policy_url         = COALESCE($(securityPolicyUrl), security_policy_url),
         vulnerability_reporting_url = CASE WHEN $(pvrResolved)
                                        THEN $(vulnerabilityReportingUrl)
                                        ELSE COALESCE($(vulnerabilityReportingUrl), vulnerability_reporting_url)
                                      END,
         bug_bounty_url              = COALESCE($(bugBountyUrl), bug_bounty_url),
         security_txt_url            = COALESCE($(securityTxtUrl), security_txt_url),
         pvr_enabled                 = COALESCE($(pvrEnabled), pvr_enabled),
         contacts_last_refreshed     = NOW()
       WHERE id = $(repoId)`,
      {
        repoId,
        securityPolicyUrl: policies.securityPolicyUrl ?? null,
        vulnerabilityReportingUrl: policies.vulnerabilityReportingUrl ?? null,
        pvrResolved: policies.pvrEnabled !== undefined,
        bugBountyUrl: policies.bugBountyUrl ?? null,
        securityTxtUrl: policies.securityTxtUrl ?? null,
        pvrEnabled: policies.pvrEnabled ?? null,
      },
    )
  })
}

export async function markReposAttempted(qx: QueryExecutor, repoIds: string[]): Promise<void> {
  if (repoIds.length === 0) return
  await qx.result(
    'UPDATE repos SET contacts_last_refreshed = NOW() WHERE id = ANY($(repoIds)::bigint[])',
    { repoIds },
  )
}

type OkResult = Extract<ProcessRepoResult, { status: 'ok' }>

// Bounds blast radius: a bad row only forces a re-extract of its chunk next sweep, not the whole batch.
const WRITE_CHUNK_SIZE = 100

function prepareBulkPolicyUpdate(chunk: OkResult[]): string {
  const rows = chunk.map(
    (_, i) =>
      `($(rows.id${i}), $(rows.securityPolicyUrl${i}), $(rows.vulnerabilityReportingUrl${i}), $(rows.pvrResolved${i}), $(rows.bugBountyUrl${i}), $(rows.securityTxtUrl${i}), $(rows.pvrEnabled${i}))`,
  )

  const params = chunk.reduce(
    (acc, r, i) => {
      acc[`id${i}`] = r.repoId
      acc[`securityPolicyUrl${i}`] = r.policies.securityPolicyUrl ?? null
      acc[`vulnerabilityReportingUrl${i}`] = r.policies.vulnerabilityReportingUrl ?? null
      acc[`pvrResolved${i}`] = r.policies.pvrEnabled !== undefined
      acc[`bugBountyUrl${i}`] = r.policies.bugBountyUrl ?? null
      acc[`securityTxtUrl${i}`] = r.policies.securityTxtUrl ?? null
      acc[`pvrEnabled${i}`] = r.policies.pvrEnabled ?? null
      return acc
    },
    {} as Record<string, unknown>,
  )

  return formatQuery(
    `UPDATE repos AS r SET
       security_policy_url         = COALESCE(v.security_policy_url, r.security_policy_url),
       vulnerability_reporting_url = CASE WHEN v.pvr_resolved
                                      THEN v.vulnerability_reporting_url
                                      ELSE COALESCE(v.vulnerability_reporting_url, r.vulnerability_reporting_url)
                                    END,
       bug_bounty_url              = COALESCE(v.bug_bounty_url, r.bug_bounty_url),
       security_txt_url            = COALESCE(v.security_txt_url, r.security_txt_url),
       pvr_enabled                 = COALESCE(v.pvr_enabled, r.pvr_enabled),
       contacts_last_refreshed     = NOW()
     FROM (VALUES ${rows.join(',')}) AS v(id, security_policy_url, vulnerability_reporting_url, pvr_resolved, bug_bounty_url, security_txt_url, pvr_enabled)
     WHERE r.id = v.id::bigint`,
    params,
  )
}

async function writeContactsChunk(qx: QueryExecutor, chunk: OkResult[]): Promise<void> {
  if (chunk.length === 0) return
  const repoIds = chunk.map((r) => r.repoId)

  await qx.tx(async (tx) => {
    await tx.result(
      'UPDATE security_contacts SET deleted_at = NOW(), updated_at = NOW() WHERE repo_id = ANY($(repoIds)::bigint[]) AND deleted_at IS NULL',
      { repoIds },
    )

    const rows = chunk.flatMap((r) => r.contacts.map((c) => toContactRow(r.repoId, c)))
    if (rows.length > 0) {
      await tx.result(
        prepareBulkInsert('security_contacts', CONTACT_COLUMNS, rows, CONTACT_UPSERT_SET),
      )
    }

    await tx.result(prepareBulkPolicyUpdate(chunk))
  })
}

export async function writeContactsBatch(
  qx: QueryExecutor,
  outcomes: ProcessRepoResult[],
): Promise<void> {
  const attemptedOnlyIds = outcomes
    .filter((o) => o.status === 'extractor-failed')
    .map((o) => o.repoId)
  const ok = outcomes.filter((o): o is OkResult => o.status === 'ok')

  for (let i = 0; i < ok.length; i += WRITE_CHUNK_SIZE) {
    const chunk = ok.slice(i, i + WRITE_CHUNK_SIZE)
    try {
      await writeContactsChunk(qx, chunk)
    } catch (err) {
      // Isolates blast radius to this chunk: without this, one bad chunk would throw out of
      // the whole batch, leaving every repo's contacts_last_refreshed untouched and the sweep
      // stuck re-fetching + re-extracting the same batch forever instead of ever advancing.
      log.error(
        { errMsg: (err as Error).message, repoIds: chunk.map((r) => r.repoId) },
        'Batched contacts write failed for chunk — marking attempted without contacts update',
      )
      attemptedOnlyIds.push(...chunk.map((r) => r.repoId))
    }
  }

  await markReposAttempted(qx, attemptedOnlyIds)
}
