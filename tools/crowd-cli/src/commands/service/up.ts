import {Command, Args, Flags} from '@oclif/core'
import {intro, outro, spinner, multiselect} from '@clack/prompts'
import {execa} from 'execa'
import {CLI_SCRIPT, SCRIPTS_PATH} from '../../lib/paths.js'
import {getAppServiceNames, isValidService} from '../../lib/services.js'

export default class ServiceUp extends Command {
  static description = 'Start a service (interactive multiselect if no name given)'

  static args = {
    name: Args.string({description: 'Service name', required: false}),
  }

  static flags = {
    all: Flags.boolean({description: 'Start all services', default: false}),
    dev: Flags.boolean({description: 'Start in dev mode (hot reload)', default: false}),
  }

  async run() {
    const {args, flags} = await this.parse(ServiceUp)

    let services: string[]

    if (flags.all) {
      services = getAppServiceNames()
    } else if (args.name) {
      if (!isValidService(args.name)) this.error(`Unknown service: ${args.name}`)
      services = [args.name]
    } else {
      if (!process.stdout.isTTY) this.error('Specify a service name or use --all in non-TTY mode')
      const selected = await multiselect({
        message: 'Select services to start',
        options: getAppServiceNames().map(s => ({value: s, label: s})),
        required: true,
      })
      if (typeof selected === 'symbol') return
      services = selected as string[]
    }

    const total = services.length
    const devLabel = flags.dev ? ' [dev]' : ''
    intro(`starting ${total} service(s)${devLabel}`)

    let started = 0
    for (let i = 0; i < services.length; i++) {
      const svc = services[i]
      const progress = total > 1 ? `[${i + 1}/${total}] ` : ''
      const s = spinner()
      s.start(`${progress}${svc}`)
      const result = await execa('bash', [CLI_SCRIPT, 'service', svc, 'up'], {
        cwd: SCRIPTS_PATH,
        env: {...process.env, ...(flags.dev ? {DEV: '1'} : {})},
        reject: false,
      })
      if (result.exitCode !== 0) {
        s.stop(`${progress}${svc} failed`)
        this.error(`service ${svc} failed to start`)
      }
      started++
      s.stop(`${progress}${svc} started`)
    }
    outro(total > 1 ? `${started}/${total} services started${devLabel}` : 'Done')
  }
}
