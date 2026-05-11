import {Command} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {scaffoldReset} from '../../lib/scaffold.js'

export default class ScaffoldReset extends Command {
  static description = 'Destroy infrastructure and restart fresh (down + up + migrations)'

  async run() {
    intro('scaffold reset')
    const s = spinner()
    s.start('Resetting infrastructure...')
    try {
      await scaffoldReset(line => s.message(line.slice(0, 70)))
      s.stop('Reset complete')
      outro('Infrastructure is fresh. Run crowd service up to start application services.')
    } catch (err) {
      s.stop('Failed')
      this.error(err instanceof Error ? err.message : String(err))
    }
  }
}
