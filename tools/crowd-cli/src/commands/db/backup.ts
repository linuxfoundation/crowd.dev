import {Command, Args} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {execa} from 'execa'
import {CLI_SCRIPT, SCRIPTS_PATH} from '../../lib/paths.js'

export default class DbBackup extends Command {
  static description = 'Backup local database to db_dumps/<name>.dump'

  static args = {
    name: Args.string({description: 'Backup name', required: true}),
  }

  async run() {
    const {args} = await this.parse(DbBackup)
    intro(`db backup: ${args.name}`)
    const s = spinner()
    s.start('Creating backup...')
    const result = await execa('bash', [CLI_SCRIPT, 'db-backup', args.name], {
      cwd: SCRIPTS_PATH,
      reject: false,
    })
    if (result.exitCode !== 0) {
      s.stop('Failed')
      this.error(result.stderr || 'backup failed')
    }
    s.stop('Backup complete')
    outro(`Saved to scripts/db_dumps/${args.name}.dump`)
  }
}
