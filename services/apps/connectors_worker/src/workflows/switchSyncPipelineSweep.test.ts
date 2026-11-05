import { beforeEach, describe, expect, it, vi } from 'vitest'

import { switchSyncPipelineSweep } from './switchSyncPipelineSweep'

const mocks = vi.hoisted(() => ({
  listSwitchSweepRepos: vi.fn(),
  executeChild: vi.fn(),
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}))

vi.mock('@temporalio/workflow', () => ({
  proxyActivities: () => ({ listSwitchSweepRepos: mocks.listSwitchSweepRepos }),
  executeChild: mocks.executeChild,
  ParentClosePolicy: { ABANDON: 'ABANDON' },
  log: mocks.log,
}))
vi.mock('./switchSyncPipeline', () => ({
  switchSyncPipeline: 'switchSyncPipeline',
  switchSyncPipelineWorkflowId: (owner: string, repo: string) => `id/${owner}/${repo}`,
}))

describe('switchSyncPipelineSweep', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('switches each repo forward in series with its own unswitched syncs, skips a repo whose switch is already running, and survives a failed child', async () => {
    mocks.listSwitchSweepRepos.mockResolvedValue([
      { owner: 'a', repo: 'one', syncNames: ['issues', 'forks'] },
      { owner: 'b', repo: 'two', syncNames: ['issues'] },
      { owner: 'c', repo: 'three', syncNames: ['pull-requests'] },
    ])
    mocks.executeChild
      .mockResolvedValueOnce([
        { syncName: 'issues', outcome: 'switched' },
        { syncName: 'forks', outcome: 'refused', reason: 'no diff summary for 2026-10-16' },
      ])
      .mockRejectedValueOnce(
        Object.assign(new Error('running'), { name: 'WorkflowExecutionAlreadyStartedError' }),
      )
      .mockRejectedValueOnce(new Error('child timed out'))

    await expect(switchSyncPipelineSweep()).resolves.toEqual({
      repos: 3,
      switched: 1,
      refused: 1,
      unchanged: 0,
      skipped: 1,
      failed: 1,
    })
    expect(mocks.executeChild.mock.calls.map(([, opts]) => opts)).toEqual([
      {
        workflowId: 'id/a/one',
        args: [
          { owner: 'a', repo: 'one', syncNames: ['issues', 'forks'], direction: 'connectors' },
        ],
        parentClosePolicy: 'ABANDON',
      },
      {
        workflowId: 'id/b/two',
        args: [{ owner: 'b', repo: 'two', syncNames: ['issues'], direction: 'connectors' }],
        parentClosePolicy: 'ABANDON',
      },
      {
        workflowId: 'id/c/three',
        args: [
          { owner: 'c', repo: 'three', syncNames: ['pull-requests'], direction: 'connectors' },
        ],
        parentClosePolicy: 'ABANDON',
      },
    ])
    expect(mocks.executeChild.mock.calls.every(([wf]) => wf === 'switchSyncPipeline')).toBe(true)
  })
})
