import { beforeEach, describe, expect, it, vi } from 'vitest'

import { queryInsightsProjects } from '@crowd/data-access-layer/src/collections'
import { listActiveOctolensKeywordMappings } from '@crowd/data-access-layer/src/octolens'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { findSegmentById } from '@crowd/data-access-layer/src/segments'

import { createOctolensDeps, resolveOctolensProjectSegmentId } from './deps'

vi.mock('@crowd/data-access-layer/src/octolens', () => ({
  listActiveOctolensKeywordMappings: vi.fn(),
}))
vi.mock('@crowd/data-access-layer/src/collections', () => ({
  InsightsProjectField: { SEGMENT_ID: 'segmentId' },
  queryInsightsProjects: vi.fn(),
}))
vi.mock('@crowd/data-access-layer/src/segments', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/data-access-layer/src/segments')>()),
  findSegmentById: vi.fn(),
}))

const mockedListMappings = vi.mocked(listActiveOctolensKeywordMappings)
const mockedQueryProjects = vi.mocked(queryInsightsProjects)
const mockedFindSegment = vi.mocked(findSegmentById)
const qx = {} as QueryExecutor

type Segment = NonNullable<Awaited<ReturnType<typeof findSegmentById>>>

beforeEach(() => {
  vi.clearAllMocks()
  mockedListMappings.mockResolvedValue([
    { integrationId: 'integration-a', segmentId: 'segment-a', keywordId: 1, keyword: 'ebpf' },
    { integrationId: 'integration-b', segmentId: 'segment-b', keywordId: 2, keyword: 'cilium' },
  ])
})

describe('createOctolensDeps', () => {
  it("resolves keywordIds to their subproject integration, not the poll's own", async () => {
    const deps = createOctolensDeps({ qx, integrationId: 'anchor' })

    expect(await deps.resolveKeywordTarget(1)).toEqual({
      segmentId: 'segment-a',
      integrationId: 'integration-a',
    })
    expect(await deps.resolveKeywordTarget(2)).toEqual({
      segmentId: 'segment-b',
      integrationId: 'integration-b',
    })
    expect(await deps.resolveKeywordTarget(3)).toBeNull()
    expect(mockedListMappings).toHaveBeenCalledWith(qx)
  })

  it('treats a keyword mapped under two integrations as unmapped', async () => {
    mockedListMappings.mockResolvedValue([
      { integrationId: 'integration-a', segmentId: 'segment-a', keywordId: 1, keyword: 'ebpf' },
      { integrationId: 'integration-b', segmentId: 'segment-b', keywordId: 1, keyword: 'ebpf' },
      { integrationId: 'integration-c', segmentId: 'segment-c', keywordId: 1, keyword: 'ebpf' },
      { integrationId: 'integration-b', segmentId: 'segment-b', keywordId: 2, keyword: 'cilium' },
    ])
    const deps = createOctolensDeps({ qx, integrationId: 'anchor' })

    expect(await deps.resolveKeywordTarget(1)).toBeNull()
    expect(await deps.resolveKeywordTarget(2)).toEqual({
      segmentId: 'segment-b',
      integrationId: 'integration-b',
    })
  })

  it('loads the mappings once per deps instance, not once per lookup', async () => {
    const deps = createOctolensDeps({ qx, integrationId: 'integration-1' })

    await deps.resolveKeywordTarget(1)
    await deps.resolveKeywordTarget(2)
    await deps.resolveKeywordTarget(1)

    expect(mockedListMappings).toHaveBeenCalledTimes(1)
  })

  it('reloads for a new deps instance, so each sync run sees fresh mappings', async () => {
    await createOctolensDeps({ qx, integrationId: 'integration-1' }).resolveKeywordTarget(1)
    await createOctolensDeps({ qx, integrationId: 'integration-1' }).resolveKeywordTarget(1)

    expect(mockedListMappings).toHaveBeenCalledTimes(2)
  })
})

describe('resolveOctolensProjectSegmentId', () => {
  it('returns the insights project segment when it is a subproject', async () => {
    mockedQueryProjects.mockResolvedValue([{ segmentId: 'segment-sub' }] as never)
    mockedFindSegment.mockResolvedValue({
      id: 'segment-sub',
      slug: 'ebpf',
      parentSlug: 'ebpf-parent',
      grandparentSlug: 'lf',
    } as Segment)

    expect(await resolveOctolensProjectSegmentId(qx, 'ebpf')).toBe('segment-sub')
    expect(mockedQueryProjects).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ filter: { slug: { eq: 'ebpf' } } }),
    )
  })

  it('returns null when the segment is not a subproject', async () => {
    mockedQueryProjects.mockResolvedValue([{ segmentId: 'segment-group' }] as never)
    mockedFindSegment.mockResolvedValue({
      id: 'segment-group',
      slug: 'lf',
      parentSlug: null,
      grandparentSlug: null,
    } as Segment)

    expect(await resolveOctolensProjectSegmentId(qx, 'lf')).toBeNull()
  })

  it('returns null when no insights project has the slug', async () => {
    mockedQueryProjects.mockResolvedValue([])

    expect(await resolveOctolensProjectSegmentId(qx, 'missing')).toBeNull()
    expect(mockedFindSegment).not.toHaveBeenCalled()
  })
})
