import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

export async function logDiscoveryRun(): Promise<void> {
  log.info('Automatic projects discovery workflow executed successfully.')
}
