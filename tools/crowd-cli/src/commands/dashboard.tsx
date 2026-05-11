import {Command} from '@oclif/core'
import {render} from 'ink'
import {spawnSync} from 'node:child_process'
import {Dashboard, PostExitAction} from '../ui/Dashboard.js'

export default class DashboardCommand extends Command {
  static description = 'Interactive dashboard (default)'
  static hidden = true

  async run() {
    if (!process.stdout.isTTY) {
      this.log('CDP Developer CLI — run without piping to see the interactive dashboard.')
      this.log('Usage: crowd <command> [subcommand] [flags]')
      return
    }
    const postExit: PostExitAction = {}
    const {waitUntilExit} = render(
      <Dashboard cliRoot={this.config.root} postExit={postExit} />
    )
    await waitUntilExit()
    if (postExit.psql) {
      const {container, db} = postExit.psql
      process.stdout.write(`\nOpening psql for ${container}...\n`)
      spawnSync('docker', ['exec', '-e', 'PGPASSWORD=example', '-it', container, 'psql', '-U', 'postgres', db], {stdio: 'inherit'})
    }
  }
}
