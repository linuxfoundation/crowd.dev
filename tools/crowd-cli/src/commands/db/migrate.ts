import {Command, Flags} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {execa} from 'execa'
import {CLI_SCRIPT, SCRIPTS_PATH} from '../../lib/paths.js'

export default class DbMigrate extends Command {
  static description = 'Run migrations (local by default, or remote with --env)'

  static flags = {
    env: Flags.boolean({description: 'Run against remote DB (uses CROWD_DB_WRITE_HOST)', default: false}),
  }

  async run() {
    const {flags} = await this.parse(DbMigrate)
    const cmd = flags.env ? 'migrate-env' : 'scaffold migrate-up'
    intro(`db migrate${flags.env ? ' [remote]' : ' [local]'}`)
    const s = spinner()
    s.start('Running migrations...')
    const result = await execa('bash', [CLI_SCRIPT, ...cmd.split(' ')], {
      cwd: SCRIPTS_PATH,
      reject: false,
    })
    if (result.exitCode !== 0) {
      s.stop('Failed')
      this.error(result.stderr || 'migration failed')
    }
    s.stop('Migrations complete')
    outro('Done.')
  }
}
