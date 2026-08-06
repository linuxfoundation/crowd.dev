import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getAnalysisDetail } from '@crowd/data-access-layer/src/packages/blastRadius'

import { runDependentsStageCargo } from '../cargo/dependentsCargo'
import { runIntelStageCargo } from '../cargo/intelCargo'
import { cargoReachabilityConfig } from '../cargo/reachabilityConfig'
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
import { runDependentsStageNuGet } from '../nuget/dependentsNuGet'
import { runIntelStageNuGet } from '../nuget/intelNuGet'
import { nugetReachabilityConfig } from '../nuget/reachabilityConfig'
import { runReachabilityStage } from '../reachability'
import { runReachabilityStage as runReachabilityStageWithConfig } from '../reachabilityStage'
import { runDependentsStageRubyGems } from '../rubygems/dependentsRubyGems'
import { runIntelStageRubyGems } from '../rubygems/intelRubyGems'
import { rubygemsReachabilityConfig } from '../rubygems/reachabilityConfig'

vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
  getAnalysisDetail: vi.fn(),
}))
vi.mock('../go/intelGo', () => ({ runIntelStageGo: vi.fn().mockResolvedValue(undefined) }))
vi.mock('../npm/intelNpm', () => ({ runIntelStageNpm: vi.fn().mockResolvedValue(undefined) }))
vi.mock('../maven/intelMaven', () => ({
  runIntelStageMaven: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../cargo/intelCargo', () => ({
  runIntelStageCargo: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../nuget/intelNuGet', () => ({
  runIntelStageNuGet: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../rubygems/intelRubyGems', () => ({
  runIntelStageRubyGems: vi.fn().mockResolvedValue(undefined),
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
vi.mock('../cargo/dependentsCargo', () => ({
  runDependentsStageCargo: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../nuget/dependentsNuGet', () => ({
  runDependentsStageNuGet: vi.fn().mockResolvedValue(undefined),
}))
vi.mock('../rubygems/dependentsRubyGems', () => ({
  runDependentsStageRubyGems: vi.fn().mockResolvedValue(undefined),
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

  it('routes intel to the Cargo body when ecosystem is cargo', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'cargo' } as never)
    await runIntelStage(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageCargo).toHaveBeenCalledWith(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageGo).not.toHaveBeenCalled()
    expect(runIntelStageNpm).not.toHaveBeenCalled()
    expect(runIntelStageMaven).not.toHaveBeenCalled()
  })

  it('routes intel to the NuGet body when ecosystem is nuget', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'nuget' } as never)
    await runIntelStage(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageNuGet).toHaveBeenCalledWith(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageGo).not.toHaveBeenCalled()
    expect(runIntelStageNpm).not.toHaveBeenCalled()
    expect(runIntelStageMaven).not.toHaveBeenCalled()
  })

  it('routes intel to the RubyGems body when ecosystem is rubygems', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'rubygems' } as never)
    await runIntelStage(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageRubyGems).toHaveBeenCalledWith(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageGo).not.toHaveBeenCalled()
    expect(runIntelStageNpm).not.toHaveBeenCalled()
    expect(runIntelStageMaven).not.toHaveBeenCalled()
  })

  it('routes intel to the npm body when ecosystem is missing/unknown', async () => {
    mockGetAnalysisDetail.mockResolvedValue(null)
    await runIntelStage(qx, 'analysis-1', 'GHSA-xxxx', undefined)
    expect(runIntelStageNpm).toHaveBeenCalled()
    expect(runIntelStageGo).not.toHaveBeenCalled()
    expect(runIntelStageMaven).not.toHaveBeenCalled()
    expect(runIntelStageNuGet).not.toHaveBeenCalled()
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

  it('routes dependents to the Cargo body when ecosystem is cargo', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'cargo' } as never)
    await runDependentsStage(qx, 'analysis-1', undefined, undefined)
    expect(runDependentsStageCargo).toHaveBeenCalled()
    expect(runDependentsStageGo).not.toHaveBeenCalled()
    expect(runDependentsStageNpm).not.toHaveBeenCalled()
    expect(runDependentsStageMaven).not.toHaveBeenCalled()
  })

  it('routes dependents to the NuGet body when ecosystem is nuget', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'nuget' } as never)
    await runDependentsStage(qx, 'analysis-1', undefined, undefined)
    expect(runDependentsStageNuGet).toHaveBeenCalled()
    expect(runDependentsStageGo).not.toHaveBeenCalled()
    expect(runDependentsStageNpm).not.toHaveBeenCalled()
    expect(runDependentsStageMaven).not.toHaveBeenCalled()
  })

  it('routes dependents to the RubyGems body when ecosystem is rubygems', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'rubygems' } as never)
    await runDependentsStage(qx, 'analysis-1', undefined, undefined)
    expect(runDependentsStageRubyGems).toHaveBeenCalled()
    expect(runDependentsStageGo).not.toHaveBeenCalled()
    expect(runDependentsStageNpm).not.toHaveBeenCalled()
    expect(runDependentsStageMaven).not.toHaveBeenCalled()
  })

  it('routes dependents to the npm body for npm/unknown ecosystems', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'npm' } as never)
    await runDependentsStage(qx, 'analysis-1', undefined, undefined)
    expect(runDependentsStageNpm).toHaveBeenCalled()
    expect(runDependentsStageGo).not.toHaveBeenCalled()
    expect(runDependentsStageMaven).not.toHaveBeenCalled()
    expect(runDependentsStageNuGet).not.toHaveBeenCalled()
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

  it('routes reachability to the Cargo config when ecosystem is cargo', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'cargo' } as never)
    await runReachabilityStage(qx, 'analysis-1', undefined)
    expect(mockRunReachabilityStageWithConfig).toHaveBeenCalledWith(
      qx,
      'analysis-1',
      cargoReachabilityConfig,
      undefined,
    )
  })

  it('routes reachability to the NuGet config when ecosystem is nuget', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'nuget' } as never)
    await runReachabilityStage(qx, 'analysis-1', undefined)
    expect(mockRunReachabilityStageWithConfig).toHaveBeenCalledWith(
      qx,
      'analysis-1',
      nugetReachabilityConfig,
      undefined,
    )
  })

  it('routes reachability to the RubyGems config when ecosystem is rubygems', async () => {
    mockGetAnalysisDetail.mockResolvedValue({ ecosystem: 'rubygems' } as never)
    await runReachabilityStage(qx, 'analysis-1', undefined)
    expect(mockRunReachabilityStageWithConfig).toHaveBeenCalledWith(
      qx,
      'analysis-1',
      rubygemsReachabilityConfig,
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
