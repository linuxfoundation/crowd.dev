import { beforeEach, describe, expect, it, vi } from 'vitest'

const findInsightsProjectIdsWithRepositories = vi.hoisted(() => vi.fn())
const startDocsReadinessWorkflow = vi.hoisted(() => vi.fn())

vi.mock('@crowd/data-access-layer/src/repositories', () => ({
  findInsightsProjectIdsWithRepositories,
}))

vi.mock('./collectionService', () => ({
  CollectionService: class {
    startDocsReadinessWorkflow = startDocsReadinessWorkflow
  },
}))

import {
  findProjectIdsGettingFirstRepository,
  startDocsReadinessForProjects,
} from './docsReadinessOnFirstRepoLink'

const qx = {} as never

describe('findProjectIdsGettingFirstRepository', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('returns a project that has no repositories yet (first link)', async () => {
    findInsightsProjectIdsWithRepositories.mockResolvedValue([])

    const result = await findProjectIdsGettingFirstRepository(qx, [{ insightsProjectId: 'p1' }])

    expect(result).toEqual(['p1'])
  })

  it('returns nothing for a project that already has a repository (second link)', async () => {
    findInsightsProjectIdsWithRepositories.mockResolvedValue(['p1'])

    const result = await findProjectIdsGettingFirstRepository(qx, [{ insightsProjectId: 'p1' }])

    expect(result).toEqual([])
  })

  it('returns a project once when several of its repositories are linked together', async () => {
    findInsightsProjectIdsWithRepositories.mockResolvedValue(['p2'])

    const result = await findProjectIdsGettingFirstRepository(qx, [
      { insightsProjectId: 'p1' },
      { insightsProjectId: 'p1' },
      { insightsProjectId: 'p2' },
    ])

    expect(findInsightsProjectIdsWithRepositories).toHaveBeenCalledWith(qx, ['p1', 'p2'])
    expect(result).toEqual(['p1'])
  })
})

describe('startDocsReadinessForProjects', () => {
  const options = { log: { error: vi.fn() } } as never

  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('starts one docs readiness run per project', async () => {
    await startDocsReadinessForProjects(options, ['p1', 'p2'])

    expect(startDocsReadinessWorkflow).toHaveBeenCalledTimes(2)
    expect(startDocsReadinessWorkflow).toHaveBeenCalledWith('p1')
    expect(startDocsReadinessWorkflow).toHaveBeenCalledWith('p2')
  })

  it('starts nothing when no project got its first repository', async () => {
    await startDocsReadinessForProjects(options, [])

    expect(startDocsReadinessWorkflow).not.toHaveBeenCalled()
  })

  it('logs and continues when a start fails', async () => {
    const error = new Error('temporal down')
    startDocsReadinessWorkflow.mockRejectedValueOnce(error)

    await expect(startDocsReadinessForProjects(options, ['p1', 'p2'])).resolves.toBeUndefined()

    expect(startDocsReadinessWorkflow).toHaveBeenCalledTimes(2)
    expect((options as { log: { error: unknown } }).log.error).toHaveBeenCalledWith(
      error,
      { projectId: 'p1' },
      expect.any(String),
    )
  })
})
