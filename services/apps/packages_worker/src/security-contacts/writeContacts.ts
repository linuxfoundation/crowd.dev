import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { prepareBulkInsert } from '@crowd/data-access-layer/src/utils'

import { RepoPolicies, ScoredContact } from './types'

// Advances contacts_last_refreshed without touching contacts/policies, so a failed pass doesn't
// wipe existing data but still isn't reprocessed this sweep.
export async function markRepoAttempted(qx: QueryExecutor, repoId: string): Promise<void> {
  await qx.result('UPDATE repos SET contacts_last_refreshed = NOW() WHERE id = $(repoId)', {
    repoId,
  })
}

// Assumes at most one writer per repoId at a time (enforced via heartbeat/cancellation in
// processBatch.ts and a non-overlapping schedule in schedule.ts) — no locking here.
//
// Soft-delete: mark every currently-active row stale, then upsert this pass's contacts, which
// revives (clears deleted_at on) whatever was rediscovered. Anything not rediscovered stays
// soft-deleted instead of being destroyed, preserving history. Readers of this table must filter
// on deleted_at IS NULL.
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
          ['repo_id', 'channel', 'value', 'role', 'name', 'score', 'confidence', 'provenance'],
          contacts.map((c) => ({
            repo_id: repoId,
            channel: c.channel,
            value: c.value,
            role: c.role,
            name: c.name ?? null,
            score: c.score,
            confidence: c.confidence,
            provenance: JSON.stringify(c.provenance),
          })),
          `(repo_id, channel, value) DO UPDATE SET
             role = EXCLUDED.role,
             name = EXCLUDED.name,
             score = EXCLUDED.score,
             confidence = EXCLUDED.confidence,
             provenance = EXCLUDED.provenance,
             last_refreshed = NOW(),
             updated_at = NOW(),
             deleted_at = NULL`,
        ),
      )
    }

    await tx.result(
      // COALESCE preserves a field a partial/failed pass didn't rediscover. vulnerability_reporting_url
      // is PVR-derived, so it's overwritten only once PVR is authoritatively resolved.
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
