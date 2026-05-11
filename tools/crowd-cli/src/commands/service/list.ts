import {Command} from '@oclif/core'
import {getContainerStatuses} from '../../lib/docker.js'
import {getAppServiceNames} from '../../lib/services.js'

export default class ServiceList extends Command {
  static description = 'List all application services and their status'

  async run() {
    await this.parse(ServiceList)
    const statuses = await getContainerStatuses()
    const services = getAppServiceNames()
    const running = services.filter(s => statuses[s]?.status === 'running' || statuses[`${s}-dev`]?.status === 'running')
    const stopped = services.filter(s => !running.includes(s))

    this.log('\nServices:')
    for (const svc of running) {
      const dev = statuses[`${svc}-dev`]?.status === 'running'
      this.log(`  ● ${svc.padEnd(36)} running${dev ? ' [dev]' : ''}`)
    }
    for (const svc of stopped) {
      this.log(`  ○ ${svc.padEnd(36)} stopped`)
    }
    this.log(`\n${running.length}/${services.length} services running`)
  }
}
