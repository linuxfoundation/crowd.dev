/* eslint-disable @typescript-eslint/dot-notation */

/* eslint-disable no-console */

/* eslint-disable import/no-extraneous-dependencies */
import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'

import { databaseInit } from '@/database/databaseConnection'
import { IRepositoryOptions } from '@/database/repositories/IRepositoryOptions'
import OrganizationRepository from '@/database/repositories/organizationRepository'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

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
    description: 'Tenant Id',
  },
]
const sections = [
  {
    header: `Fix empty displayName in organizations`,
    content: 'Script will fix organizations with empty displayName',
  },
  {
    header: 'Options',
    optionList: options,
  },
]

const usage = commandLineUsage(sections)
const parameters = commandLineArgs(options)

function getOrgsWithoutDisplayName(
  qx: QueryExecutor,
  tenantId: string,
  { limit = 50, countOnly = false },
) {
  return qx.select(
    `
        SELECT
        ${countOnly ? 'COUNT(*)' : 'o.id'}
        FROM organizations o
        WHERE o."tenantId" = $(tenantId)
        AND o."displayName" IS NULL
        ${countOnly ? '' : 'LIMIT $(limit)'}
    `,
    { tenantId, limit },
  )
}

async function getOrgIdentities(qx: QueryExecutor, orgId: string, tenantId: string) {
  return qx.select(
    `
      SELECT value
      FROM "organizationIdentities"
      WHERE "organizationId" = $(orgId)
      AND "tenantId" = $(tenantId)
      LIMIT 1
    `,
    { orgId, tenantId },
  )
}

async function getOrgAttributes(qx: QueryExecutor, orgId: string) {
  return qx.select(
    `
      SELECT value
      FROM "orgAttributes"
      WHERE "organizationId" = $(orgId)
      AND name = 'name'
      LIMIT 1
    `,
    { orgId },
  )
}

async function updateOrgDisplayName(qx: QueryExecutor, orgId: string, displayName: string) {
  await qx.result(
    `
      UPDATE organizations
      SET "displayName" = $(displayName)
      WHERE id = $(id)
    `,
    { id: orgId, displayName },
  )
}

if (parameters.help || !parameters.tenantId) {
  console.log(usage)
} else {
  setImmediate(async () => {
    const prodDb = await databaseInit()
    const tenantId = parameters.tenantId
    const qx = SequelizeRepository.getQueryExecutor({
      database: prodDb,
    } as IRepositoryOptions)

    const options = await SequelizeRepository.getDefaultIRepositoryOptions()

    const BATCH_SIZE = 50
    let processed = 0

    const totalOrgs = await getOrgsWithoutDisplayName(qx, tenantId, { countOnly: true })

    console.log(`Total organizations without displayName: ${totalOrgs[0].count}`)

    let orgs = await getOrgsWithoutDisplayName(qx, tenantId, { limit: BATCH_SIZE })

    while (totalOrgs[0].count > processed) {
      for (const org of orgs) {
        let displayName
        let updateAttributes = false

        const attributes = await getOrgAttributes(qx, org.id)

        if (attributes.length > 0) {
          displayName = attributes[0]?.value
        } else {
          const identities = await getOrgIdentities(qx, org.id, tenantId)
          displayName = identities && identities[0]?.value
          updateAttributes = true
        }

        if (displayName) {
          await updateOrgDisplayName(qx, org.id, displayName)

          if (updateAttributes) {
            await OrganizationRepository.updateOrgAttributes(
              org.id,
              {
                attributes: {
                  name: {
                    custom: [displayName],
                    default: displayName,
                  },
                },
              },
              options,
            )
          }
        } else {
          console.log(`Organization ${org.id} does not have displayName`)
        }

        processed++
      }

      console.log(`Processed ${processed}/${totalOrgs[0].count} organizations`)

      orgs = await getOrgsWithoutDisplayName(qx, tenantId, { limit: BATCH_SIZE })
    }

    process.exit(0)
  })
}
