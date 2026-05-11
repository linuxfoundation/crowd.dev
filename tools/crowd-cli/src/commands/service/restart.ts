import {Command, Args, Flags} from '@oclif/core'
import {intro, outro, spinner, multiselect} from '@clack/prompts'
import {execa} from 'execa'
import {CLI_SCRIPT, SCRIPTS_PATH} from '../../lib/paths.js'
import {getAppServiceNames, isValidService} from '../../lib/services.js'

export default class ServiceRestart extends Command {
  static description = 'Restart a service (interactive multiselect if no name given)'

  static args = {
    name: Args.string({description: 'Service name', required: false}),
  }

  static flags = {
    dev: Flags.boolean({description: 'Target dev container', default: false}),
  }

  async run() {
    const {args, flags} = await this.parse(ServiceRestart)

    let services: string[]
    if (args.name) {
      if (!isValidService(args.name)) this.error(`Unknown service: ${args.name}`)
      services = [args.name]
    } else {
      if (!process.stdout.isTTY) this.error('Specify a service name in non-TTY mode')
      const selected = await multiselect({
        message: 'Select services to restart',
        options: getAppServiceNames().map(s => ({value: s, label: s})),
        required: true,
      })
      if (typeof selected === 'symbol') return
      services = selected as string[]
    }

    const total = services.length
    const devLabel = flags.dev ? ' [dev]' : ''
    intro(`restarting ${total} service(s)${devLabel}`)

    let restarted = 0
    for (let i = 0; i < services.length; i++) {
      const svc = services[i]
      const progress = total > 1 ? `[${i + 1}/${total}] ` : ''
      const s = spinner()
      s.start(`${progress}${svc}`)
      await execa('bash', [CLI_SCRIPT, 'service', svc, 'down'], {
        cwd: SCRIPTS_PATH,
        env: {...process.env, ...(flags.dev ? {DEV: '1'} : {})},
        reject: false,
      })
      const result = await execa('bash', [CLI_SCRIPT, 'service', svc, 'up'], {
        cwd: SCRIPTS_PATH,
        env: {...process.env, ...(flags.dev ? {DEV: '1'} : {})},
        reject: false,
      })
      if (result.exitCode !== 0) {
        s.stop(`${progress}${svc} failed`)
        this.error(`service ${svc} failed to restart`)
      }
      restarted++
      s.stop(`${progress}${svc} restarted`)
    }
    outro(total > 1 ? `${restarted}/${total} services restarted${devLabel}` : 'Done')
  }
}
