import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('npm')

export async function sayHiNpm(): Promise<string> {
  log.info('👋 hi from npm activity')
  return 'hi from npm'
}
