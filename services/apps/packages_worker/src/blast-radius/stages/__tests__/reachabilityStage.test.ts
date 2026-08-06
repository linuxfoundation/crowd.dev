import { beforeEach, describe, expect, it, vi } from 'vitest'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'

import { runAnalysisAgent } from '../../agent/runner'
import { ReachabilitySourceConfig, runReachabilityStage } from '../reachabilityStage'

vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
  getStageRunStatus: vi.fn(),
  startStageRun: vi.fn(),
  getSymbolSpec: vi.fn(),
  getDependentsNeedingVerdict: vi.fn(),
  upsertVerdict: vi.fn(),
  getVerdictsCost: vi.fn(),
  completeStageRun: vi.fn(),
  failStageRun: vi.fn(),
}))
vi.mock('../../agent/runner', () => ({ runAnalysisAgent: vi.fn() }))

const qx = {} as never
const SYMBOL_SPEC_ROW = {
  vuln_id: 'GHSA-xxxx',
  package: 'pkg',
  summary: 'summary',
  vulnerable_symbols: [],
  import_signatures: {},
  exploit_preconditions: '',
  reachability_notes: '',
  confidence: 0.9,
}
const DEPENDENT = {
  id: 'dep-1',
  analysis_id: 'analysis-1',
  name: 'some-dep',
  excluded_by_range: false,
  tarball_url: null,
}

function makeConfig(overrides: Partial<ReachabilitySourceConfig> = {}): ReachabilitySourceConfig {
  return {
    prompt: 'prompt',
    schema: {},
    buildSystemPrompt: () => 'system prompt',
    prepareSource: vi.fn().mockResolvedValue({ download: vi.fn().mockResolvedValue(undefined) }),
    noSourceMessage: 'no source',
    downloadErrorPrefix: 'download failed',
    ...overrides,
  }
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.mocked(blastRadiusDal.getStageRunStatus).mockResolvedValue('pending' as never)
  vi.mocked(blastRadiusDal.getSymbolSpec).mockResolvedValue(SYMBOL_SPEC_ROW as never)
  vi.mocked(blastRadiusDal.getDependentsNeedingVerdict).mockResolvedValue([DEPENDENT] as never)
  vi.mocked(blastRadiusDal.getVerdictsCost).mockResolvedValue(0)
})

describe('runReachabilityStage', () => {
  it('returns early without touching dependents when the stage already succeeded', async () => {
    vi.mocked(blastRadiusDal.getStageRunStatus).mockResolvedValue('succeeded' as never)
    await runReachabilityStage(qx, 'analysis-1', makeConfig())
    expect(blastRadiusDal.getDependentsNeedingVerdict).not.toHaveBeenCalled()
  })

  it('persists an error verdict with noSourceMessage when prepareSource returns null', async () => {
    const cfg = makeConfig({ prepareSource: vi.fn().mockResolvedValue(null) })
    await runReachabilityStage(qx, 'analysis-1', cfg)

    expect(blastRadiusDal.upsertVerdict).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ dependentId: 'dep-1', reasoning: 'no source', model: null }),
    )
    expect(runAnalysisAgent).not.toHaveBeenCalled()
  })

  it('persists an error verdict with downloadErrorPrefix when download throws', async () => {
    const cfg = makeConfig({
      prepareSource: vi
        .fn()
        .mockResolvedValue({ download: vi.fn().mockRejectedValue(new Error('boom')) }),
    })
    await runReachabilityStage(qx, 'analysis-1', cfg)

    expect(blastRadiusDal.upsertVerdict).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({
        dependentId: 'dep-1',
        reasoning: 'download failed: boom',
      }),
    )
    expect(runAnalysisAgent).not.toHaveBeenCalled()
  })

  it('persists the mapped verdict on a successful agent run', async () => {
    vi.mocked(runAnalysisAgent).mockResolvedValue({
      structuredOutput: {
        uses_package: true,
        imports_vulnerable_symbol: true,
        import_style: 'plain-import',
        reachable_verdict: 'affected',
        confidence: 0.8,
        evidence: [],
        reasoning: 'found it',
      },
      isError: false,
      errorMessage: '',
      numTurns: 3,
      costUsd: 0.05,
    })

    await runReachabilityStage(qx, 'analysis-1', makeConfig())

    expect(blastRadiusDal.upsertVerdict).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({
        dependentId: 'dep-1',
        usesPackage: true,
        reachableVerdict: 'affected',
        importStyle: 'plain-import',
        model: 'claude-sonnet-5',
        turnsUsed: 3,
        costUsd: 0.05,
      }),
    )
    expect(blastRadiusDal.completeStageRun).toHaveBeenCalled()
  })

  it('retries on agent error and succeeds on a later attempt', async () => {
    vi.mocked(runAnalysisAgent)
      .mockResolvedValueOnce({
        structuredOutput: null,
        isError: true,
        errorMessage: 'transient',
        numTurns: 1,
        costUsd: 0.01,
      })
      .mockResolvedValueOnce({
        structuredOutput: {
          uses_package: false,
          imports_vulnerable_symbol: false,
          import_style: 'none',
          reachable_verdict: 'not_affected',
          confidence: 0.7,
          evidence: [],
          reasoning: 'not reached',
        },
        isError: false,
        errorMessage: '',
        numTurns: 2,
        costUsd: 0.02,
      })

    await runReachabilityStage(qx, 'analysis-1', makeConfig())

    expect(runAnalysisAgent).toHaveBeenCalledTimes(2)
    expect(blastRadiusDal.upsertVerdict).toHaveBeenCalledWith(
      qx,
      expect.objectContaining({ dependentId: 'dep-1', reachableVerdict: 'not_affected' }),
    )
  }, 20_000)
})
