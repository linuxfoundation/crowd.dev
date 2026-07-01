import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { RepoPolicies, ScoredContact } from './types'

// Records the attempt without touching contacts/policies — preserves existing data on total
// failure while advancing contacts_last_refreshed so the repo isn't reprocessed this sweep.
export async function markRepoAttempted(qx: QueryExecutor, repoId: string): Promise<void> {
  await qx.result('UPDATE repos SET contacts_last_refreshed = NOW() WHERE id = $(repoId)', {
    repoId,
  })
}

/**
 * Idempotent per-repo recompute: replace the repo's security_contacts rows and refresh
 * the policy columns in one transaction. Policy columns use COALESCE so a run that doesn't
 * rediscover a field (partial/failed extractor pass) never clears a previously known value.
 */
export async function writeContacts(
  qx: QueryExecutor,
  repoId: string,
  contacts: ScoredContact[],
  policies: Partial<RepoPolicies>,
): Promise<void> {
  await qx.tx(async (tx) => {
    await tx.result('DELETE FROM security_contacts WHERE repo_id = $(repoId)', { repoId })

    for (const c of contacts) {
      await tx.result(
        `INSERT INTO security_contacts
           (repo_id, channel, value, role, name, score, confidence, provenance, last_refreshed)
         VALUES
           ($(repoId), $(channel), $(value), $(role), $(name), $(score), $(confidence), $(provenance)::jsonb, NOW())`,
        {
          repoId,
          channel: c.channel,
          value: c.value,
          role: c.role,
          name: c.name ?? null,
          score: c.score,
          confidence: c.confidence,
          provenance: JSON.stringify(c.provenance),
        },
      )
    }

    await tx.result(
      // COALESCE preserves previously stored values when a run doesn't (re)discover a field —
      // a partial/failed extractor pass must not wipe still-valid policy URLs or the PVR flag.
      `UPDATE repos SET
         security_policy_url         = COALESCE($(securityPolicyUrl), security_policy_url),
         vulnerability_reporting_url = COALESCE($(vulnerabilityReportingUrl), vulnerability_reporting_url),
         bug_bounty_url              = COALESCE($(bugBountyUrl), bug_bounty_url),
         security_txt_url            = COALESCE($(securityTxtUrl), security_txt_url),
         pvr_enabled                 = COALESCE($(pvrEnabled), pvr_enabled),
         contacts_last_refreshed     = NOW()
       WHERE id = $(repoId)`,
      {
        repoId,
        securityPolicyUrl: policies.securityPolicyUrl ?? null,
        vulnerabilityReportingUrl: policies.vulnerabilityReportingUrl ?? null,
        bugBountyUrl: policies.bugBountyUrl ?? null,
        securityTxtUrl: policies.securityTxtUrl ?? null,
        pvrEnabled: policies.pvrEnabled ?? null,
      },
    )
  })
}
