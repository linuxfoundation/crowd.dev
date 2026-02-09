import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

export async function logDiscoveryRun(): Promise<void> {
  log.info('Automatic project discovery workflow executed successfully.')
}
