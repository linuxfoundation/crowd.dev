import { ParentClosePolicy, WorkflowIdReusePolicy, startChild } from '@temporalio/workflow'

import { ITriggerNangoIntegrationCheckArguments } from '@crowd/nango'

import { processNangoWebhook } from './processNangoWebhook'

export async function triggerNangoIntegrationCheck(
  args: ITriggerNangoIntegrationCheckArguments,
): Promise<void> {
  for (const connection of args.connections) {
    for (const model of connection.models) {
      const workflowId = connection.workflowIdPrefix
        ? `nango-webhook/${args.providerConfigKey}/${args.integrationId}/${connection.workflowIdPrefix}/${model}/cron-triggered`
        : `nango-webhook/${args.providerConfigKey}/${args.integrationId}/${model}/cron-triggered`

      try {
        await startChild(processNangoWebhook, {
          workflowId,
          parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
          workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
          args: [
            {
              connectionId: connection.connectionId,
              providerConfigKey: args.providerConfigKey,
              model,
              syncType: 'INCREMENTAL' as const,
            },
          ],
        })
      } catch (err) {
        if (err instanceof Error && err.name === 'WorkflowExecutionAlreadyStartedError') {
          continue
        }
        throw err
      }
    }
  }
}
