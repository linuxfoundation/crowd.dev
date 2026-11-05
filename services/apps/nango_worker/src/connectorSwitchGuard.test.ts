import { beforeEach, describe, expect, it, vi } from 'vitest'

import { isSwitchedToConnectors } from './connectorSwitchGuard'

const mocks = vi.hoisted(() => ({
  hasEmittingSyncUnit: vi.fn(),
  getNangoMappingByConnectionId: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/connectors', () => ({
  hasEmittingSyncUnit: mocks.hasEmittingSyncUnit,
}))
vi.mock('@crowd/data-access-layer/src/integrations', () => ({
  getNangoMappingByConnectionId: mocks.getNangoMappingByConnectionId,
}))

const qx = {} as never

describe('isSwitchedToConnectors', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.getNangoMappingByConnectionId.mockResolvedValue({
      owner: 'Kubernetes',
      repoName: 'kubernetes',
    })
  })

  it('drops a model only when an emitting connector unit exists for its sync on that repo', async () => {
    mocks.hasEmittingSyncUnit.mockResolvedValue(true)

    await expect(
      isSwitchedToConnectors(qx, 'int-1', 'conn-1', 'GithubPullRequestReviewThreadComment'),
    ).resolves.toBe(true)
    expect(mocks.hasEmittingSyncUnit).toHaveBeenCalledWith(
      qx,
      'int-1',
      'https://github.com/Kubernetes/kubernetes',
      'pull-request-review-comments',
    )

    mocks.hasEmittingSyncUnit.mockResolvedValue(false)
    await expect(isSwitchedToConnectors(qx, 'int-1', 'conn-1', 'GithubIssue')).resolves.toBe(false)
  })

  it('never drops stars or a connection without a mapping row', async () => {
    await expect(isSwitchedToConnectors(qx, 'int-1', 'conn-1', 'GithubStar')).resolves.toBe(false)
    expect(mocks.getNangoMappingByConnectionId).not.toHaveBeenCalled()

    mocks.getNangoMappingByConnectionId.mockResolvedValue(null)
    await expect(isSwitchedToConnectors(qx, 'int-1', 'conn-1', 'GithubIssue')).resolves.toBe(false)
    expect(mocks.hasEmittingSyncUnit).not.toHaveBeenCalled()
  })
})
