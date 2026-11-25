import { SegmentStatus } from '@crowd/types'

import type { CreationEndpoint, CreationStep } from './creationSteps'

export interface BackendApiConfig {
  apiUrl: string
  apiToken: string
}

export interface SegmentCreationPayload {
  name: string
  slug: string
  sourceId: string
  isLF: true
  status?: SegmentStatus
  parentSlug?: string
  parentName?: string
  grandparentSlug?: string
  grandparentName?: string
}

// The backend answers after its commit: with a null `httpStatus` (no answer) or a 5xx the
// segment may exist all the same.
export type SegmentCreationResult =
  | { outcome: 'created' }
  | { outcome: 'failed'; httpStatus: number | null; error: string }

const REQUEST_TIMEOUT_MS = 30_000
const ERROR_BODY_MAX_LENGTH = 500

const ENDPOINT_PATH: Record<CreationEndpoint, string> = {
  projectGroup: '/segment/projectGroup',
  project: '/segment/project',
  subproject: '/segment/subproject',
}

export function readBackendApiConfig(): BackendApiConfig | null {
  const apiUrl = process.env.CROWD_API_SERVICE_URL
  const apiToken = process.env.CROWD_LF_AGENT_USER_TOKEN

  if (!apiUrl || !apiToken) return null

  return { apiUrl: apiUrl.replace(/\/+$/, ''), apiToken }
}

export function buildSegmentCreationPayload(
  step: CreationStep,
  status: SegmentStatus | null = null,
): SegmentCreationPayload {
  return {
    name: step.node.name,
    slug: step.node.slug,
    sourceId: step.node.pccProjectId,
    isLF: true,
    ...(status && { status }),
    ...(step.parent && { parentSlug: step.parent.slug, parentName: step.parent.name }),
    ...(step.grandparent && {
      grandparentSlug: step.grandparent.slug,
      grandparentName: step.grandparent.name,
    }),
  }
}

async function readErrorBody(response: Response): Promise<string> {
  try {
    const body = await response.text()
    return body.length > ERROR_BODY_MAX_LENGTH ? `${body.slice(0, ERROR_BODY_MAX_LENGTH)}…` : body
  } catch {
    return ''
  }
}

function describeRequestError(err: unknown): string {
  if (!(err instanceof Error)) return String(err)
  const { cause } = err as Error & { cause?: unknown }
  return cause instanceof Error ? `${err.message}: ${cause.message}` : err.message
}

export async function createSegment(
  config: BackendApiConfig,
  step: CreationStep,
  status: SegmentStatus | null = null,
): Promise<SegmentCreationResult> {
  let response: Response
  try {
    response = await fetch(`${config.apiUrl}${ENDPOINT_PATH[step.endpoint]}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${config.apiToken}` },
      body: JSON.stringify(buildSegmentCreationPayload(step, status)),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    })
  } catch (err) {
    return { outcome: 'failed', httpStatus: null, error: describeRequestError(err) }
  }

  if (!response.ok) {
    const body = await readErrorBody(response)
    return {
      outcome: 'failed',
      httpStatus: response.status,
      error: [`HTTP ${response.status}`, body].filter(Boolean).join(': '),
    }
  }

  await response.body?.cancel().catch(() => undefined)
  return { outcome: 'created' }
}
