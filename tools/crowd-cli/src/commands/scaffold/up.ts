import {Command} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {scaffoldUp} from '../../lib/scaffold.js'

export default class ScaffoldUp extends Command {
  static description = 'Start all infrastructure services (postgres, redis, kafka, temporal, tinybird, ...)'

  async run() {
    intro('scaffold up')
    const s = spinner()
    s.start('Starting infrastructure...')
    try {
      await scaffoldUp(line => {
        s.message(line.slice(0, 70))
      })
      s.stop('Infrastructure started')
      outro('Done. Run crowd service up to start application services.')
    } catch (err) {
      s.stop('Failed')
      this.error(err instanceof Error ? err.message : String(err))
    }
  }
}
