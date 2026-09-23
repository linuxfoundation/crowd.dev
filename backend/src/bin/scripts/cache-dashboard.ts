import { randomUUID } from 'crypto'
import * as fs from 'fs'
import path from 'path'

import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'

import { TEMPORAL_CONFIG } from '@/conf'
import { getTemporalClient } from '@crowd/temporal'

/* eslint-disable no-console */

const banner = fs.readFileSync(path.join(__dirname, 'banner.txt'), 'utf8')

const options = [
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
  {
    name: 'tenantId',
    alias: 't',
    type: String,
    description: 'Tenant ID',
  },
  {
    name: 'allTenants',
    alias: 'a',
    type: Boolean,
    description: 'all tenants',
  },
]
const sections = [
  {
    content: banner,
    raw: true,
  },
  {
    header: `Cache dashboard to redis for given tenants`,
    content: 'Cache dashboard to redis for given tenants',
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
    const temporal = await getTemporalClient(TEMPORAL_CONFIG)

    const uuid = randomUUID()

    await temporal.workflow.start('spawnDashboardCacheRefreshForAllTenants', {
      taskQueue: 'cache',
      workflowId: `spawnDashboardCacheRefreshForAllTenants/${uuid}`,
      retry: {
        maximumAttempts: 10,
      },
      args: [],
      searchAttributes: {},
    })

    process.exit(0)
  })
}
