import {Command, Args, Flags} from '@oclif/core'
import {intro, outro} from '@clack/prompts'
import {execa} from 'execa'
import {CLI_SCRIPT, SCRIPTS_PATH} from '../../lib/paths.js'

export default class ScaffoldCreateMigration extends Command {
  static description = 'Generate a new Flyway migration file pair (V + U scripts)'

  static args = {
    name: Args.string({description: 'Migration name (snake_case)', required: true}),
  }

  static flags = {
    product: Flags.boolean({description: 'Create in product DB instead of main DB', default: false}),
  }

  async run() {
    const {args, flags} = await this.parse(ScaffoldCreateMigration)
    intro('scaffold create-migration')
    const cmd = flags.product ? 'scaffold create-product-migration' : 'scaffold create-migration'
    const result = await execa('bash', [CLI_SCRIPT, ...cmd.split(' '), args.name], {
      cwd: SCRIPTS_PATH,
      reject: false,
    })
    if (result.exitCode !== 0) {
      outro('Failed.')
      this.error(result.stderr || 'Failed to create migration')
    }
    this.log(result.stdout)
    outro(`Migration created.`)
  }
}
