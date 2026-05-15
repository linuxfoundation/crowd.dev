import {Command, Args} from '@oclif/core'
import {getContainerStatuses} from '../../lib/docker.js'
import {isValidService} from '../../lib/services.js'

export default class ServiceStatus extends Command {
  static description = 'Show status of a service'

  static args = {
    name: Args.string({description: 'Service name', required: true}),
  }

  async run() {
    const {args} = await this.parse(ServiceStatus)
    if (!isValidService(args.name)) this.error(`Unknown service: ${args.name}`)
    const statuses = await getContainerStatuses()
    const info = statuses[args.name]
    if (!info) {
      this.log(`${args.name}: stopped`)
    } else {
      this.log(`${args.name}: ${info.status} (${info.state})`)
    }
  }
}
