import { createHash } from 'crypto'
import type { Request, Response } from 'express'

import { NotFoundError } from '@crowd/common'
import { getContactDetailsByPurls } from '@crowd/data-access-layer'
import { WorkflowIdConflictPolicy, WorkflowIdReusePolicy } from '@crowd/temporal'
import { TemporalWorkflowId } from '@crowd/types'

import { getPackagesQx } from '@/db/packagesDb'
import { getPackagesTemporalClient } from '@/db/packagesTemporal'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { toAkritesExternalContactDetail } from './akritesExternalContactDetail'
import { purlBodySchema } from './purl'

interface IngestSecurityContactsForPurlResult {
  found: boolean
  repoId?: string
}

// Deterministic, purl-derived workflowId: concurrent callers hitting the same purl attach
// to the same running workflow (USE_EXISTING) instead of each starting their own ingest —
// same pattern as integrationService.ts's github-nango-sync workflow start.
function ingestWorkflowId(purl: string): string {
  return `${TemporalWorkflowId.SECURITY_CONTACTS_ONDEMAND}:${createHash('sha256').update(purl).digest('hex')}`
}

// Sync, single-purl on-demand ingest. Blocks on the worker's single-repo activity
// (security-contacts/workflows.ts's singleActs: 45s startToCloseTimeout x 2 attempts,
// worst case ~95s, plus unbounded time waiting for a free worker slot) — no batch
// variant, since fanning this out over many purls would multiply concurrent Temporal
// workflow starts.
//
// The underlying workflow itself has no "already processed" gate — it reprocesses and
// replaces a repo's contacts on every invocation — so this handler is the one place
// that must avoid re-triggering it for a purl that's already been ingested. Gate on
// repos.contacts_last_refreshed: null means never processed (trigger the workflow),
// non-null means a pass already ran (return what's there, even if it found no contacts).
export async function ingestAkritesExternalContactDetail(
  req: Request,
  res: Response,
): Promise<void> {
  const { purl } = validateOrThrow(purlBodySchema, req.body)

  const qx = await getPackagesQx()
  const [existing] = await getContactDetailsByPurls(qx, [purl])
  if (existing?.contactsLastRefreshed) {
    ok(res, toAkritesExternalContactDetail(existing))
    return
  }

  const packagesTemporal = await getPackagesTemporalClient()
  const result = await packagesTemporal.workflow.execute<
    (purl: string) => Promise<IngestSecurityContactsForPurlResult>
  >('ingestSecurityContactsForPurlWorkflow', {
    taskQueue: 'security-contacts-worker',
    workflowId: ingestWorkflowId(purl),
    workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
    workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
    args: [purl],
  })

  if (!result.found) {
    throw new NotFoundError('Purl has no linked repository to ingest security contacts from')
  }

  const [row] = await getContactDetailsByPurls(qx, [purl])

  if (!row) {
    throw new NotFoundError('Contact detail not found after ingest')
  }

  ok(res, toAkritesExternalContactDetail(row))
}
