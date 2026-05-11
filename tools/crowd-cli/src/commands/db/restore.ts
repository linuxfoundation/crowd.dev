import {Command, Args} from '@oclif/core'
import {intro, outro, spinner, confirm, cancel} from '@clack/prompts'
import {execa} from 'execa'
import {CLI_SCRIPT, SCRIPTS_PATH} from '../../lib/paths.js'

export default class DbRestore extends Command {
  static description = 'Restore database from db_dumps/<name>.dump (destroys current data)'

  static args = {
    name: Args.string({description: 'Backup name', required: true}),
  }

  async run() {
    const {args} = await this.parse(DbRestore)
    intro(`db restore: ${args.name}`)
    const ok = await confirm({
      message: 'This will destroy all current database data. Continue?',
      initialValue: false,
    })
    if (!ok) {
      cancel('Aborted.')
      return
    }
    const s = spinner()
    s.start('Restoring...')
    const result = await execa('bash', [CLI_SCRIPT, 'db-restore', args.name], {
      cwd: SCRIPTS_PATH,
      reject: false,
    })
    if (result.exitCode !== 0) {
      s.stop('Failed')
      this.error(result.stderr || 'restore failed')
    }
    s.stop('Restore complete')
    outro('Database restored.')
  }
}
