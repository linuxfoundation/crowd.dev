import {Command} from '@oclif/core'
import {intro, outro, spinner} from '@clack/prompts'
import {execa} from 'execa'
import {REPO_ROOT} from '../../lib/paths.js'
import {resolve} from 'path'

export default class LintBackend extends Command {
  static description = 'Lint backend (eslint + prettier + tsc)'

  async run() {
    intro('lint backend')
    const backendDir = resolve(REPO_ROOT, 'backend')

    for (const [label, args] of [
      ['eslint', ['pnpm', 'lint']],
      ['format-check', ['pnpm', 'format-check']],
      ['tsc', ['pnpm', 'tsc-check']],
    ] as Array<[string, string[]]>) {
      const s = spinner()
      s.start(label)
      const result = await execa(args[0], args.slice(1), {cwd: backendDir, reject: false})
      if (result.exitCode !== 0) {
        s.stop(`${label} failed`)
        this.log(result.stdout)
        this.log(result.stderr)
        this.error(`${label} failed`)
      }
      s.stop(`${label} passed`)
    }
    outro('All backend checks passed.')
  }
}
