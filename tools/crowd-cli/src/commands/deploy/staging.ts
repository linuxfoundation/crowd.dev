import {Command} from '@oclif/core'
import {intro, outro, spinner, multiselect} from '@clack/prompts'
import {execa} from 'execa'
import {SCRIPTS_PATH} from '../../lib/paths.js'
import {getAppServiceNames} from '../../lib/services.js'

export default class DeployStaging extends Command {
  static description = 'Deploy selected services to staging via GitHub Actions'

  async run() {
    intro('deploy staging')
    const selected = await multiselect({
      message: 'Select services to deploy',
      options: getAppServiceNames().map(s => ({value: s, label: s})),
      required: true,
    })
    if (typeof selected === 'symbol') return

    const s = spinner()
    s.start('Triggering staging deploy workflow...')
    const result = await execa('gh', [
      'workflow', 'run', 'lf-oracle-staging-deploy.yaml',
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
