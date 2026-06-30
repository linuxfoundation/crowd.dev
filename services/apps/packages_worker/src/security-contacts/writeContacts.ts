import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { RepoPolicies, ScoredContact } from './types'

/**
 * Idempotent per-repo recompute: replace the repo's security_contacts rows and refresh
 * the policy columns in one transaction. pvr_enabled uses COALESCE so an "unknown" result
 * (A2 endpoint failure) never clears a previously known value.
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
      `UPDATE repos SET
         security_policy_url         = $(securityPolicyUrl),
         vulnerability_reporting_url = $(vulnerabilityReportingUrl),
         bug_bounty_url              = $(bugBountyUrl),
         security_txt_url            = $(securityTxtUrl),
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
