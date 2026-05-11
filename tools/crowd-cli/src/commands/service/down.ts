import {Command, Args, Flags} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {execa} from 'execa'
import {CLI_SCRIPT, SCRIPTS_PATH} from '../../lib/paths.js'
import {isValidService} from '../../lib/services.js'

export default class ServiceDown extends Command {
  static description = 'Stop a service'

  static args = {
    name: Args.string({description: 'Service name', required: true}),
  }

  static flags = {
    dev: Flags.boolean({description: 'Target dev container', default: false}),
  }

  async run() {
    const {args, flags} = await this.parse(ServiceDown)
    if (!isValidService(args.name)) this.error(`Unknown service: ${args.name}`)
    intro(`service down: ${args.name}`)
    const s = spinner()
    s.start(`Stopping ${args.name}...`)
    const result = await execa('bash', [CLI_SCRIPT, 'service', args.name, 'down'], {
      cwd: SCRIPTS_PATH,
      env: {...process.env, ...(flags.dev ? {DEV: '1'} : {})},
      reject: false,
    })
    if (result.exitCode !== 0) {
      s.stop(`Failed`)
      this.error(result.stderr || `service ${args.name} stop failed`)
    }
    s.stop(`${args.name} stopped`)
    outro('Done')
  }
}
