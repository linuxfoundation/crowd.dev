/* eslint-disable no-console */

/* eslint-disable import/no-extraneous-dependencies */
import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import AuthService from '@/services/auth/authService'

const options = [
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
  {
    name: 'email',
    alias: 'e',
    type: String,
    defaultValue: 'local-dev@example.com',
    description: 'Email for the dev user driving the onboarding (default: local-dev@example.com)',
  },
]

const sections = [
  {
    header: 'Onboard default tenant',
    content:
      'Runs the same onboarding logic as a real Auth0 signup (AuthService.signinFromSSO -> ' +
      'handleOnboard -> TenantService.createOrJoinDefault), without needing a real Auth0 token. ' +
      'Creates the default tenant, default segment, settings, and an admin user/tenantUser row. ' +
      'Safe to re-run — createOrJoinDefault joins the existing tenant instead of duplicating it.',
  },
  {
    header: 'Options',
    optionList: options,
  },
]

const usage = commandLineUsage(sections)
const parameters = commandLineArgs(options)

if (parameters.help) {
  console.log(usage)
} else {
  setImmediate(async () => {
    const repoOptions = await SequelizeRepository.getDefaultIRepositoryOptions()

    await AuthService.signinFromSSO(
      'auth0',
      `dev|${parameters.email}`,
      parameters.email,
      true,
      'Local',
      'Dev',
      'Local Dev',
      null,
      null,
      null,
      repoOptions,
    )

    const tenant = await repoOptions.database.tenant.findOne({ where: {} })
    const segment = await repoOptions.database.segment.findOne({
      where: { tenantId: tenant.id },
    })

    console.log(`Tenant: ${tenant.id} (${tenant.name})`)
    console.log(`Segment: ${segment.id} (${segment.name})`)

    process.exit(0)
  })
}
