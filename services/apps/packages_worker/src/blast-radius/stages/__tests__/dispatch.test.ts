import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getAnalysisDetail } from '@crowd/data-access-layer/src/packages/blastRadius'

import { runDependentsStage } from '../dependents'
import { runDependentsStageGo } from '../go/dependentsGo'
import { runIntelStageGo } from '../go/intelGo'
import { goReachabilityConfig } from '../go/reachabilityConfig'
import { runIntelStage } from '../intel'
import { runDependentsStageMaven } from '../maven/dependentsMaven'
import { runIntelStageMaven } from '../maven/intelMaven'
import { mavenReachabilityConfig } from '../maven/reachabilityConfig'
import { runDependentsStageNpm } from '../npm/dependentsNpm'
import { runIntelStageNpm } from '../npm/intelNpm'
import { npmReachabilityConfig } from '../npm/reachabilityConfig'
import { runReachabilityStage } from '../reachability'
import { runReachabilityStage as runReachabilityStageWithConfig } from '../reachabilityStage'

vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
  getAnalysisDetail: vi.fn(),
}))
vi.mock('../go/intelGo', () => ({ runIntelStageGo: vi.fn().mockResolvedValue(undefined) }))
vi.mock('../npm/intelNpm', () => ({ runIntelStageNpm: vi.fn().mockResolvedValue(undefined) }))
vi.mock('../maven/intelMaven', () => ({
  runIntelStageMaven: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../go/dependentsGo', () => ({
  runDependentsStageGo: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../npm/dependentsNpm', () => ({
  runDependentsStageNpm: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../maven/dependentsMaven', () => ({
  runDependentsStageMaven: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../reachabilityStage', () => ({
  runReachabilityStage: vi.fn().mockResolvedValue(undefined),
}))

const mockRunReachabilityStageWithConfig = vi.mocked(runReachabilityStageWithConfig)

const mockGetAnalysisDetail = vi.mocked(getAnalysisDetail)
const qx = {} as never

beforeEach(() => {
  vi.clearAllMocks()
})

describe('stage dispatchers (EcosystemConfig registry)', () => {
  it('routes intel to the Go body when ecosystem is go', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'go' } as never)
    await runIntelStage(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageGo).toHaveBeenCalledWith(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageNpm).not.toHaveBeenCalled()
    expect(runIntelStageMaven).not.toHaveBeenCalled()
  })

  it('routes intel to the npm body when ecosystem is npm', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'npm' } as never)
    await runIntelStage(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageNpm).toHaveBeenCalledWith(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageGo).not.toHaveBeenCalled()
    expect(runIntelStageMaven).not.toHaveBeenCalled()
  })

  it('routes intel to the Maven body when ecosystem is maven', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'maven' } as never)
    await runIntelStage(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageMaven).toHaveBeenCalledWith(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageGo).not.toHaveBeenCalled()
    expect(runIntelStageNpm).not.toHaveBeenCalled()
  })

  it('routes intel to the npm body when ecosystem is missing/unknown', async () => {
    mockGetAnalysisDetail.mockResolvedValue(null)
    await runIntelStage(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageNpm).toHaveBeenCalled()
    expect(runIntelStageGo).not.toHaveBeenCalled()
    expect(runIntelStageMaven).not.toHaveBeenCalled()
  })

  it('routes dependents to the Go body when ecosystem is go', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'go' } as never)
    await runDependentsStage(qx, 'analysis-1', undefined, undefined)
    expect(runDependentsStageGo).toHaveBeenCalled()
    expect(runDependentsStageNpm).not.toHaveBeenCalled()
    expect(runDependentsStageMaven).not.toHaveBeenCalled()
  })

  it('routes dependents to the Maven body when ecosystem is maven', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'maven' } as never)
    await runDependentsStage(qx, 'analysis-1', undefined, undefined)
    expect(runDependentsStageMaven).toHaveBeenCalled()
    expect(runDependentsStageGo).not.toHaveBeenCalled()
    expect(runDependentsStageNpm).not.toHaveBeenCalled()
  })

  it('routes dependents to the npm body for npm/unknown ecosystems', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'npm' } as never)
    await runDependentsStage(qx, 'analysis-1', undefined, undefined)
    expect(runDependentsStageNpm).toHaveBeenCalled()
    expect(runDependentsStageGo).not.toHaveBeenCalled()
    expect(runDependentsStageMaven).not.toHaveBeenCalled()
  })

  it('routes reachability to the Go config when ecosystem is go', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'go' } as never)
    await runReachabilityStage(qx, 'analysis-1', undefined)
    expect(mockRunReachabilityStageWithConfig).toHaveBeenCalledWith(
      qx,
      'analysis-1',
      goReachabilityConfig,
      undefined,
    )
  })

  it('routes reachability to the Maven config when ecosystem is maven', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'maven' } as never)
    await runReachabilityStage(qx, 'analysis-1', undefined)
    expect(mockRunReachabilityStageWithConfig).toHaveBeenCalledWith(
      qx,
      'analysis-1',
      mavenReachabilityConfig,
      undefined,
    )
  })

  it('routes reachability to the npm config for npm/unknown ecosystems', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'npm' } as never)
    await runReachabilityStage(qx, 'analysis-1', undefined)
    expect(mockRunReachabilityStageWithConfig).toHaveBeenCalledWith(
      qx,
      'analysis-1',
      npmReachabilityConfig,
      undefined,
    )
  })
})
