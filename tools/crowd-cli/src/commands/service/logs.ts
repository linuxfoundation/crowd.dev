import {Command, Args, Flags} from '@oclif/core'
import {execa} from 'execa'
import {CLI_SCRIPT, SCRIPTS_PATH} from '../../lib/paths.js'
import {isValidService} from '../../lib/services.js'

export default class ServiceLogs extends Command {
  static description = 'Stream logs from a service'

  static args = {
    name: Args.string({description: 'Service name', required: true}),
  }

  static flags = {
    dev: Flags.boolean({description: 'Target dev container', default: false}),
  }

  async run() {
    const {args, flags} = await this.parse(ServiceLogs)
    if (!isValidService(args.name)) this.error(`Unknown service: ${args.name}`)
    await execa('bash', [CLI_SCRIPT, 'service', args.name, 'logs'], {
      cwd: SCRIPTS_PATH,
      env: {...process.env, ...(flags.dev ? {DEV: '1'} : {})},
      stdio: 'inherit',
      reject: false,
    })
  }
}
