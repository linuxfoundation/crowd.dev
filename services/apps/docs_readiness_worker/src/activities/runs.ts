import {
  DocReadinessRunScope,
  DocReadinessRunTrigger,
  IDocReadinessRunFinish,
  finishDocReadinessRun,
  startDocReadinessRun,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { svc } from '../main'

export interface IStartRunArgs {
  trigger: DocReadinessRunTrigger
  scope: DocReadinessRunScope
  workflowId?: string | null
  temporalRunId?: string | null
}

export async function startRun(args: IStartRunArgs): Promise<string> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const run = await startDocReadinessRun(qx, args)
  return run.id
}

export async function finishRun(runId: string, data: IDocReadinessRunFinish): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  await finishDocReadinessRun(qx, runId, data)
}
