import {Command} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {scaffoldMigrate} from '../../lib/scaffold.js'

export default class ScaffoldMigrate extends Command {
  static description = 'Run all local migrations (Postgres, ProductDB, Tinybird)'

  async run() {
    intro('scaffold migrate')
    const s = spinner()
    s.start('Running migrations...')
    try {
      await scaffoldMigrate(line => {
        s.message(line.slice(0, 70))
      })
      s.stop('Migrations complete')
      outro('All migrations applied.')
    } catch (err) {
      s.stop('Failed')
      this.error(err instanceof Error ? err.message : String(err))
    }
  }
}
