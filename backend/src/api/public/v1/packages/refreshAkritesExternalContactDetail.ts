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
function refreshWorkflowId(purl: string): string {
  return `${TemporalWorkflowId.SECURITY_CONTACTS_ONDEMAND}:${createHash('sha256').update(purl).digest('hex')}`
}

// Sync, single-purl on-demand ingest. Blocks ~10-20s (the worker's single-repo bound —
// see security-contacts/workflows.ts's singleActs timeout) — no batch variant, since
// fanning this out over many purls would multiply concurrent Temporal workflow starts.
export async function refreshAkritesExternalContactDetail(
  req: Request,
  res: Response,
): Promise<void> {
  const { purl } = validateOrThrow(purlBodySchema, req.body)

  const packagesTemporal = await getPackagesTemporalClient()
  const result = await packagesTemporal.workflow.execute<
    (purl: string) => Promise<IngestSecurityContactsForPurlResult>
  >('ingestSecurityContactsForPurlWorkflow', {
    taskQueue: 'security-contacts-worker',
    workflowId: refreshWorkflowId(purl),
    workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
    workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
    args: [purl],
  })

  if (!result.found) {
    throw new NotFoundError()
  }

  const qx = await getPackagesQx()
  const [row] = await getContactDetailsByPurls(qx, [purl])

  if (!row) {
    throw new NotFoundError()
  }

  ok(res, toAkritesExternalContactDetail(row))
}
