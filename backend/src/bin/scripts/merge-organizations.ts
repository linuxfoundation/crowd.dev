import * as fs from 'fs'
import path from 'path'

import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'

import OrganizationRepository from '@/database/repositories/organizationRepository'
import OrganizationService from '@/services/organizationService'
import { getServiceLogger } from '@crowd/logging'

import SequelizeRepository from '../../database/repositories/sequelizeRepository'

/* eslint-disable no-console */

const banner = fs.readFileSync(path.join(__dirname, 'banner.txt'), 'utf8')

const log = getServiceLogger()

const options = [
  {
    name: 'tenantId',
    alias: 't',
    typeLabel: '{underline tenantId}',
    type: String,
    description: 'The tenantId both organizations belongs to',
  },
  {
    name: 'originalId',
    alias: 'o',
    typeLabel: '{underline originalId}',
    type: String,
    description:
      'The unique ID of an organization that will be kept. The other will be merged into this one.',
  },
  {
    name: 'toMergeId',
    alias: 'm',
    typeLabel: '{underline toMergeId}',
    type: String,
    description:
      'The unique ID of an organization that will be merged into the first one. This one will be destroyed. You can provide multiple ids here separated by comma.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]
const sections = [
  {
    content: banner,
    raw: true,
  },
  {
    header: 'Merge two organizations',
    content: 'Merge two organizations so that only one remains. The other one will be destroyed.',
  },
  {
    header: 'Options',
    optionList: options,
  },
]

const usage = commandLineUsage(sections)
const parameters = commandLineArgs(options)

if (parameters.help || !parameters.originalId || !parameters.toMergeId || !parameters.tenantId) {
  console.log(usage)
} else {
  setImmediate(async () => {
    const tenantId = parameters.tenantId
    const originalId = parameters.originalId
    const toMergeIds = parameters.toMergeId.split(',')

    const options = await SequelizeRepository.getDefaultIRepositoryOptions()

    options.currentTenant = { id: tenantId }

    const originalOrganizaton = await OrganizationRepository.findById(originalId, options)

    for (const toMergeId of toMergeIds) {
      const toMergeOrganization = await OrganizationRepository.findById(toMergeId, options)

      if (originalOrganizaton.tenantId !== toMergeOrganization.tenantId) {
        log.error(
          `Organizations ${originalId} and ${toMergeId} are not from the same tenant. Will not merge!`,
        )
      } else {
        log.info(`Merging ${toMergeId} into ${originalId}...`)
        const service = new OrganizationService(options)
        try {
          await service.mergeSync(originalId, toMergeId, null)
        } catch (err) {
          log.error(`Error merging organizations: ${err.message}`)
          process.exit(1)
        }
      }
    }

    process.exit(0)
  })
}
