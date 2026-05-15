import {Command} from '@oclif/core'
import {intro, outro, spinner, multiselect, confirm, cancel, isCancel} from '@clack/prompts'
import {execa} from 'execa'
import {SCRIPTS_PATH} from '../../lib/paths.js'
import {getAppServiceNames} from '../../lib/services.js'

export default class DeployProduction extends Command {
  static description = 'Deploy selected services to production via GitHub Actions'

  async run() {
    intro('deploy production')
    const selected = await multiselect({
      message: 'Select services to deploy to PRODUCTION',
      options: getAppServiceNames().map(s => ({value: s, label: s})),
      required: true,
    })
    if (typeof selected === 'symbol') return

    const ok = await confirm({
      message: `Deploy ${(selected as string[]).length} service(s) to PRODUCTION?`,
      initialValue: false,
    })
    if (isCancel(ok) || !ok) {
      cancel('Aborted.')
      return
    }

    const s = spinner()
    s.start('Triggering production deploy workflow...')
    const result = await execa('gh', [
      'workflow', 'run', 'lf-oracle-production-deploy.yaml',
      '--field', `services=${(selected as string[]).join(',')}`,
    ], {
      cwd: SCRIPTS_PATH + '/..',
      reject: false,
    })
    if (result.exitCode !== 0) {
      s.stop('Failed')
      this.error(result.stderr || 'gh workflow run failed')
    }
    s.stop('Workflow triggered')
    outro('Check GitHub Actions for deploy progress.')
  }
}
