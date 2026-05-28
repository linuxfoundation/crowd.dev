import { getServiceChildLogger } from '@crowd/logging'

export * from './activities/index'

const log = getServiceChildLogger('deps-dev')

export async function depsDevPlaceholder(): Promise<void> {
  log.info('deps-dev placeholder activity')
}
