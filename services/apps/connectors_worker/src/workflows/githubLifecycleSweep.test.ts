import { beforeEach, describe, expect, it, vi } from 'vitest'

import { githubLifecycleSweep } from './githubLifecycleSweep'

const mocks = vi.hoisted(() => ({
  listIntegrationsNeedingReconcile: vi.fn(),
  executeChild: vi.fn(),
  isCancellation: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => ({
    listIntegrationsNeedingReconcile: mocks.listIntegrationsNeedingReconcile,
  }),
  executeChild: mocks.executeChild,
  ParentClosePolicy: { ABANDON: 'ABANDON' },
  isCancellation: mocks.isCancellation,
  log: mocks.log,
}))
vi.mock('./reconcileGithubIntegration', () => ({
  reconcileGithubIntegration: 'reconcileGithubIntegration',
}))

describe('githubLifecycleSweep', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.isCancellation.mockReturnValue(false)
  })

  it('reconciles integrations five at a time, skips ones already running and survives a failed child', async () => {
    const ids = ['i1', 'i2', 'i3', 'i4', 'i5', 'i6', 'i7']
    mocks.listIntegrationsNeedingReconcile.mockResolvedValue(ids)
    const releaseFirstChunk: Array<() => void> = []
    mocks.executeChild.mockImplementation(
      (_workflow, opts: { args: [{ integrationId: string }] }) => {
        const { integrationId } = opts.args[0]
        if (integrationId === 'i6') {
          return Promise.reject(
            Object.assign(new Error('running'), { name: 'WorkflowExecutionAlreadyStartedError' }),
          )
        }
        if (integrationId === 'i7') {
          return Promise.reject(new Error('child timed out'))
        }
        return new Promise<void>((resolve) => releaseFirstChunk.push(resolve))
      },
    )

    const sweep = githubLifecycleSweep()
    await vi.waitFor(() => expect(mocks.executeChild).toHaveBeenCalledTimes(5))
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(mocks.executeChild).toHaveBeenCalledTimes(5)

    releaseFirstChunk.forEach((release) => release())

    await expect(sweep).resolves.toEqual({ integrations: 7, reconciled: 5, skipped: 1, failed: 1 })
    expect(mocks.executeChild.mock.calls.map(([workflow, opts]) => [workflow, opts])).toEqual(
      ids.map((id) => [
        'reconcileGithubIntegration',
        {
          workflowId: `connectors-reconcile-github-integration/${id}`,
          args: [{ integrationId: id }],
          parentClosePolicy: 'ABANDON',
          workflowExecutionTimeout: '4 hours',
        },
      ]),
    )
    expect(mocks.log.error).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ integrationId: 'i7', error: 'child timed out' }),
    )

    vi.clearAllMocks()
    mocks.listIntegrationsNeedingReconcile.mockResolvedValue(['i1', 'i2'])
    const cancelled = new Error('cancelled')
    mocks.isCancellation.mockImplementation((err) => err === cancelled)
    mocks.executeChild.mockRejectedValue(cancelled)
    await expect(githubLifecycleSweep()).rejects.toBe(cancelled)
  })
})
