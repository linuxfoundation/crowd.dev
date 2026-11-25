import { afterEach, describe, expect, test, vi } from 'vitest'

import { SegmentStatus } from '@crowd/types'

import type { CreationStep } from './creationSteps'
import {
  buildSegmentCreationPayload,
  createSegment,
  readBackendApiConfig,
} from './segmentCreationClient'

const CONFIG = { apiUrl: 'https://cdp.test/api', apiToken: 'agent-token' }

const CNCF = { slug: 'cncf', name: 'CNCF' }
const KUBERNETES = { slug: 'k8s', name: 'Kubernetes' }

const groupStep: CreationStep = {
  endpoint: 'projectGroup',
  node: { pccProjectId: 'pcc-zephyr', name: 'Zephyr Project', slug: 'zephyr' },
}
const projectStep: CreationStep = {
  endpoint: 'project',
  node: { pccProjectId: 'pcc-kubernetes', name: 'Kubernetes', slug: 'k8s' },
  parent: CNCF,
}
const subprojectStep: CreationStep = {
  endpoint: 'subproject',
  node: { pccProjectId: 'pcc-kubectl', name: 'kubectl', slug: 'kubectl' },
  parent: KUBERNETES,
  grandparent: CNCF,
}

function stubFetch(response: Response | Error) {
  const fetchMock = vi.fn(async () => {
    if (response instanceof Error) throw response
    return response
  })
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

function sentRequest(fetchMock: ReturnType<typeof stubFetch>) {
  const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit]
  return { url, init, body: JSON.parse(init.body as string) }
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.unstubAllEnvs()
})

describe('readBackendApiConfig', () => {
  test('reads the API url and the agent token', () => {
    vi.stubEnv('CROWD_API_SERVICE_URL', 'https://cdp.test/api/')
    vi.stubEnv('CROWD_LF_AGENT_USER_TOKEN', 'agent-token')

    expect(readBackendApiConfig()).toEqual(CONFIG)
  })

  test.each([
    ['the API url', 'CROWD_API_SERVICE_URL'],
    ['the agent token', 'CROWD_LF_AGENT_USER_TOKEN'],
  ])('returns null without %s', (_label, missing) => {
    vi.stubEnv('CROWD_API_SERVICE_URL', 'https://cdp.test/api')
    vi.stubEnv('CROWD_LF_AGENT_USER_TOKEN', 'agent-token')
    vi.stubEnv(missing, '')

    expect(readBackendApiConfig()).toBeNull()
  })
})

describe('buildSegmentCreationPayload', () => {
  test('links a project group to its PCC project and marks it as LF', () => {
    expect(buildSegmentCreationPayload(groupStep)).toEqual({
      name: 'Zephyr Project',
      slug: 'zephyr',
      sourceId: 'pcc-zephyr',
      isLF: true,
    })
  })

  test('sends the slug and the name of the group a project goes under', () => {
    expect(buildSegmentCreationPayload(projectStep)).toMatchObject({
      parentSlug: 'cncf',
      parentName: 'CNCF',
    })
  })

  test('sends the project and the group a sub-project goes under', () => {
    expect(buildSegmentCreationPayload(subprojectStep)).toMatchObject({
      parentSlug: 'k8s',
      parentName: 'Kubernetes',
      grandparentSlug: 'cncf',
      grandparentName: 'CNCF',
    })
  })

  test('sends the status only when PCC has one', () => {
    expect(buildSegmentCreationPayload(groupStep, SegmentStatus.FORMATION).status).toBe(
      SegmentStatus.FORMATION,
    )
    expect(buildSegmentCreationPayload(groupStep, null)).not.toHaveProperty('status')
  })
})

describe('createSegment', () => {
  test.each([
    [groupStep, 'https://cdp.test/api/segment/projectGroup'],
    [projectStep, 'https://cdp.test/api/segment/project'],
    [subprojectStep, 'https://cdp.test/api/segment/subproject'],
  ])('posts a $endpoint step to its own endpoint', async (step, expectedUrl) => {
    const fetchMock = stubFetch(new Response('{}', { status: 200 }))

    const result = await createSegment(CONFIG, step)

    const { url, init, body } = sentRequest(fetchMock)
    expect(result).toEqual({ outcome: 'created' })
    expect(url).toBe(expectedUrl)
    expect(init.method).toBe('POST')
    expect(init.headers).toMatchObject({ Authorization: 'Bearer agent-token' })
    expect(body).toEqual(buildSegmentCreationPayload(step))
  })

  test('reports the status and the body when the backend refuses the segment', async () => {
    stubFetch(new Response('Segment slug already exists', { status: 400 }))

    const result = await createSegment(CONFIG, projectStep)

    expect(result).toEqual({
      outcome: 'failed',
      httpStatus: 400,
      error: 'HTTP 400: Segment slug already exists',
    })
  })

  test('cuts a long error body', async () => {
    stubFetch(new Response('x'.repeat(2000), { status: 500 }))

    const result = await createSegment(CONFIG, projectStep)

    expect(result.outcome).toBe('failed')
    expect(result.outcome === 'failed' && result.error.length).toBeLessThan(600)
  })

  test('reports no status when the backend gives no answer', async () => {
    stubFetch(new Error('The operation was aborted due to timeout'))

    const result = await createSegment(CONFIG, projectStep)

    expect(result).toEqual({
      outcome: 'failed',
      httpStatus: null,
      error: 'The operation was aborted due to timeout',
    })
  })

  test('reports the cause of a failed request', async () => {
    stubFetch(
      Object.assign(new TypeError('fetch failed'), { cause: new Error('connect ECONNREFUSED') }),
    )

    const result = await createSegment(CONFIG, projectStep)

    expect(result).toMatchObject({ error: 'fetch failed: connect ECONNREFUSED' })
  })
})
