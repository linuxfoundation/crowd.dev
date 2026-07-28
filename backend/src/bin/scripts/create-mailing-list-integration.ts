/* eslint-disable no-console */

/* eslint-disable import/no-extraneous-dependencies */
import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'
import * as fs from 'fs'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'

import {
  bodySchema,
  canonicalizeSourceUrl,
} from '@/api/integration/helpers/mailingListAuthenticate'
import SegmentRepository from '@/database/repositories/segmentRepository'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import IntegrationService from '@/services/integrationService'
import { validateOrThrow } from '@/utils/validation'

const options = [
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
  {
    name: 'file',
    alias: 'f',
    type: String,
    description:
      'Path to a JSON file with the lists array, each entry shaped as name + sourceUrl ' +
      '(e.g. name "linux-serial", sourceUrl https://lore.kernel.org/linux-serial).',
  },
  {
    name: 'segment',
    alias: 's',
    type: String,
    description: "Segment id. Optional — defaults to the tenant's first subproject.",
  },
]

const sections = [
  {
    header: 'Create mailing list integration',
    content:
      'Calls IntegrationService.mailingListConnectOrUpdate() directly to create/update the ' +
      'mailinglist integration and onboard its lists for processing, until the frontend connect ' +
      'UI ships (CM-1318).',
  },
  {
    header: 'Options',
    optionList: options,
  },
]

const usage = commandLineUsage(sections)
// pnpm forwards a literal `--` separator when args are passed via `pnpm run ... -- <args>`;
// strip it so command-line-args doesn't choke on it.
const argv = process.argv.slice(2).filter((arg) => arg !== '--')
const parameters = commandLineArgs(options, { argv })

if (parameters.help || !parameters.file) {
  console.log(usage)
  process.exit(parameters.help ? 0 : 1)
} else {
  setImmediate(async () => {
    let fileContents: string
    try {
      fileContents = fs.readFileSync(parameters.file, 'utf-8')
    } catch (err) {
      console.error(`Could not read file at ${parameters.file}: ${(err as Error).message}`)
      process.exit(1)
    }

    let parsed: unknown
    try {
      parsed = JSON.parse(fileContents)
    } catch (err) {
      console.error(`File at ${parameters.file} is not valid JSON: ${(err as Error).message}`)
      process.exit(1)
    }

    // Same validation as the connect API endpoint (mailingListAuthenticate.ts),
    // so a malformed file (non-array, missing fields, duplicate/non-https
    // sourceUrl) fails fast here instead of reaching IntegrationService with
    // whatever shape the file happened to contain.
    const { lists: parsedLists } = validateOrThrow(bodySchema, { lists: parsed })
    const lists = parsedLists.map((l) => ({ ...l, sourceUrl: canonicalizeSourceUrl(l.sourceUrl) }))

    const repoOptions = await SequelizeRepository.getDefaultIRepositoryOptions()
    repoOptions.currentTenant = { id: DEFAULT_TENANT_ID }
    ;(repoOptions as unknown as { requestId: string }).requestId = generateUUIDv1()

    const adminUser = await repoOptions.database.user.findOne({ where: {} })
    if (!adminUser) {
      console.error('No user found — run script:onboard-default-tenant first.')
      process.exit(1)
    }
    repoOptions.currentUser = adminUser

    const segmentRepository = new SegmentRepository(repoOptions)
    repoOptions.currentSegments = parameters.segment
      ? await segmentRepository.findInIds([parameters.segment])
      : (await segmentRepository.querySubprojects({ limit: 1, offset: 0 })).rows

    if (repoOptions.currentSegments.length === 0) {
      console.error('No segment found/resolved — pass --segment explicitly.')
      process.exit(1)
    }

    console.log(
      `Segment: ${repoOptions.currentSegments[0].id} (${repoOptions.currentSegments[0].name})`,
    )
    console.log('Lists:', JSON.stringify(lists, null, 2))

    const integration = await new IntegrationService(repoOptions).mailingListConnectOrUpdate(
      { lists },
      repoOptions,
    )

    console.log('Integration:', JSON.stringify(integration, null, 2))
    process.exit(0)
  })
}
