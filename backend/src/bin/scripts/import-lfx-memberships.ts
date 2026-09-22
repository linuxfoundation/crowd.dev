/* eslint-disable @typescript-eslint/dot-notation */

/* eslint-disable no-console */

import * as fs from 'fs'
import path from 'path'

/* eslint-disable import/no-extraneous-dependencies */
import commandLineArgs from 'command-line-args'
import commandLineUsage from 'command-line-usage'
import { parse } from 'csv-parse/sync'
import uniq from 'lodash/uniq'
import moment from 'moment'

import { databaseInit } from '@/database/databaseConnection'
import { IRepositoryOptions } from '@/database/repositories/IRepositoryOptions'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { LfxMembership, insertLfxMembership } from '@crowd/data-access-layer/src/lfx_memberships'
import {
  findOrgIdByDisplayName,
  findOrgIdByDomain,
} from '@crowd/data-access-layer/src/organizations'
import { findProjectGroupByName } from '@crowd/data-access-layer/src/segments'

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
    description: 'Path to CSV file to import',
  },
  {
    name: 'tenantId',
    alias: 't',
    type: String,
    description: 'Tenant Id. Hint: what you probably need is 875c38bd-2b1b-4e91-ad07-0cfbabb4c49f',
  },
]
const sections = [
  {
    header: `Import LFX Membership `,
    content:
      'Merges two members, then unmerges these and cross checks unmerge result with original data.',
  },
  {
    header: 'Options',
    optionList: options,
  },
]

const usage = commandLineUsage(sections)
const parameters = commandLineArgs(options)

function parseDomains(domains: string) {
  return uniq(
    domains
      .split(',')
      .map((domain) => domain.trim())
      .filter((domain) => domain.length > 0)

      // the rest if for values that look like this: "andesdigital.cl\n\n--- Merged Data:\n\ andesdigital.cl"
      .flatMap((domain) => domain.split('\n'))
      .filter((domain) => domain.match(/^[a-z0-9.-]+$/)),
  )
}

async function findOrgId(qx, record) {
  let org = await findOrgIdByDomain(qx, [record['Account Domain']])
  if (org) {
    return org
  }

  org = await findOrgIdByDomain(qx, record['Domain Alias'])
  if (org) {
    return org
  }

  org = await findOrgIdByDisplayName(qx, { orgName: record['Account Name'], exact: true })
  if (org) {
    return org
  }

  org = await findOrgIdByDisplayName(qx, {
    orgName: record['Account Name'],
    exact: false,
  })
  return org
}

if (parameters.help || !parameters.file || !parameters.tenantId) {
  console.log(usage)
} else {
  setImmediate(async () => {
    const prodDb = await databaseInit()
    const qx = SequelizeRepository.getQueryExecutor({
      database: prodDb,
    } as IRepositoryOptions)

    await qx.result(`DELETE FROM "lfxMemberships"`)

    console.log('All records deleted')

    const fileData = fs.readFileSync(path.resolve(parameters.file), 'latin1')

    const records = parse(fileData, {
      columns: true,
      skip_empty_lines: true,
    })

    console.log('New records:', records.length)

    for (let i = 0; i < records.length; i++) {
      const record = records[i]
      const orgName = record['Account Name']

      // Exclude individual no account organizations from LF Members
      if (
        ![
          'Individual - No Account',
          'Individual ? No  Account',
          'individual with no account',
        ].includes(orgName)
      ) {
        record['Domain Alias'] = parseDomains(record['Domain Alias'])

        const segment = await findProjectGroupByName(qx, {
          name: record['Project'],
        })
        const orgId = await findOrgId(qx, record)
        const row = {
          organizationId: orgId,
          segmentId: segment?.id,
          accountName: orgName,
          parentAccount: record['Parent Account'],
          project: record['Project'],
          productName: record['Product Name'],
          purchaseHistoryName: record['Purchase History Name'],
          installDate: moment(record['Install Date'], 'MM/DD/YYYY').toDate(),
          usageEndDate: moment(record['Usage End Date'], 'MM/DD/YYYY').toDate(),
          status: record['Status'],
          priceCurrency: record['Price Currency'],
          price: parseInt(record['Price'], 10),
          productFamily: record['Product Family'],
          tier: record['Tier'],
          accountDomain: record['Account Domain'],
          domainAlias: record['Domain Alias'],
        } as LfxMembership

        await insertLfxMembership(qx, row)

        console.log('Inserted record:', i, orgName)
      } else {
        console.log('Ignored Individual - No account:', i, orgName)
      }
    }

    process.exit(0)
  })
}
