import {Command} from '@oclif/core'
import {intro, outro, spinner, confirm, cancel, isCancel} from '@clack/prompts'
import {scaffoldDestroy} from '../../lib/scaffold.js'

export default class ScaffoldDestroy extends Command {
  static description = 'Stop infrastructure and DELETE all volumes (irreversible)'

  async run() {
    intro('scaffold destroy')
    const ok = await confirm({
      message: 'This will permanently delete all Docker volumes. Continue?',
      initialValue: false,
    })
    if (isCancel(ok) || !ok) {
      cancel('Aborted.')
      return
    }
    const s = spinner()
    s.start('Destroying infrastructure...')
    try {
      await scaffoldDestroy(line => s.message(line.slice(0, 70)))
      s.stop('Infrastructure destroyed')
      outro('All volumes deleted.')
    } catch (err) {
      s.stop('Failed')
      this.error(err instanceof Error ? err.message : String(err))
    }
  }
}
