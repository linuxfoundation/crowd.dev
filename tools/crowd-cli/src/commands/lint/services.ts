import {Command} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {execa} from 'execa'
import {SCRIPTS_PATH} from '../../lib/paths.js'
import {resolve} from 'path'

export default class LintServices extends Command {
  static description = 'Lint all services (apps + libs)'

  async run() {
    intro('lint services')
    const servicesDir = resolve(SCRIPTS_PATH, '..', 'services')

    for (const [label, script] of [
      ['lint apps', './lint_apps.sh'],
      ['lint libs', './lint_libs.sh'],
    ]) {
      const s = spinner()
      s.start(label)
      const result = await execa('bash', [script], {cwd: servicesDir, reject: false})
      if (result.exitCode !== 0) {
        s.stop(`${label} failed`)
        this.log(result.stdout)
        this.log(result.stderr)
        this.error(`${label} failed`)
      }
      s.stop(`${label} passed`)
    }
    outro('All service checks passed.')
  }
}
