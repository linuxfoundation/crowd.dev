import {Command} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {scaffoldDown} from '../../lib/scaffold.js'

export default class ScaffoldDown extends Command {
  static description = 'Stop infrastructure services (preserves volumes)'

  async run() {
    intro('scaffold down')
    const s = spinner()
    s.start('Stopping infrastructure...')
    try {
      await scaffoldDown(line => s.message(line.slice(0, 70)))
      s.stop('Infrastructure stopped')
      outro('Volumes preserved. Run crowd scaffold up to restart.')
    } catch (err) {
      s.stop('Failed')
      this.error(err instanceof Error ? err.message : String(err))
    }
  }
}
