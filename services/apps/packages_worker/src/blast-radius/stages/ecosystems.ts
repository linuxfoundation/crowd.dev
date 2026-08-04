import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { Ecosystem } from '../ecosystemSupport'

import { runDependentsStageCargo } from './cargo/dependentsCargo'
import { runIntelStageCargo } from './cargo/intelCargo'
import { cargoReachabilityConfig } from './cargo/reachabilityConfig'
import { runDependentsStageGo } from './go/dependentsGo'
import { runIntelStageGo } from './go/intelGo'
import { goReachabilityConfig } from './go/reachabilityConfig'
import { runDependentsStageMaven } from './maven/dependentsMaven'
import { runIntelStageMaven } from './maven/intelMaven'
import { mavenReachabilityConfig } from './maven/reachabilityConfig'
import { runDependentsStageNpm } from './npm/dependentsNpm'
import { runIntelStageNpm } from './npm/intelNpm'
import { npmReachabilityConfig } from './npm/reachabilityConfig'
import { ReachabilitySourceConfig } from './reachabilityStage'

// Replaces the 3 scattered `if (ecosystem === 'go')` dispatch branches. Record<Ecosystem, …>
// enforces at compile time that every SUPPORTED_ECOSYSTEMS entry has a config.
interface EcosystemConfig {
  runIntel: (
    qx: QueryExecutor,
    analysisId: string,
    advisoryOsvId: string,
    onProgress?: () => void,
  ) => Promise<void>
  runDependents: (
    qx: QueryExecutor,
    analysisId: string,
    onProgress?: () => void,
    signal?: AbortSignal,
  ) => Promise<void>
  reachability: ReachabilitySourceConfig
}

const ECOSYSTEMS: Record<Ecosystem, EcosystemConfig> = {
  npm: {
    runIntel: runIntelStageNpm,
    runDependents: runDependentsStageNpm,
    reachability: npmReachabilityConfig,
  },
  go: {
    runIntel: runIntelStageGo,
    runDependents: runDependentsStageGo,
    reachability: goReachabilityConfig,
  },
  maven: {
    runIntel: runIntelStageMaven,
    runDependents: runDependentsStageMaven,
    reachability: mavenReachabilityConfig,
  },
  cargo: {
    runIntel: runIntelStageCargo,
    runDependents: runDependentsStageCargo,
    reachability: cargoReachabilityConfig,
  },
}

export function getEcosystemConfig(ecosystem: string | null | undefined): EcosystemConfig {
  return ECOSYSTEMS[ecosystem as Ecosystem] ?? ECOSYSTEMS.npm
}
