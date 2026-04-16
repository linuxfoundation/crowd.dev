import * as readline from 'readline'

import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { OrganizationIdentityType } from '@crowd/types'

import { DB_CONFIG } from '../conf'

/* eslint-disable @typescript-eslint/no-explicit-any */

const log = getServiceLogger()

const BATCH_SIZE = 100
const ERROR_PATTERN =
  '%Missing organization identity or displayName while creating/updating organization%'

function fixOrg(org: any, platform: string): any {
  const fixed: any = { ...org }

  // Map name → displayName
  if (!fixed.displayName && fixed.name) {
    fixed.displayName = fixed.name
  }

  // Drop identities missing value or type
  fixed.identities = (org.identities ?? []).filter((i: any) => i.value && i.type)

  // Inject website as PRIMARY_DOMAIN identity if no verified domain exists
  const hasVerifiedDomain = fixed.identities.some(
    (i: any) => i.verified && i.type === OrganizationIdentityType.PRIMARY_DOMAIN,
  )
  if (org.website && !hasVerifiedDomain) {
    fixed.identities.push({
      platform,
      value: org.website,
      type: OrganizationIdentityType.PRIMARY_DOMAIN,
      verified: true,
      source: platform,
    })
  }

  return fixed
}

function isOrgValid(org: any): boolean {
  const hasDisplayName = !!org.displayName
  const hasVerifiedIdentity = (org.identities ?? []).some(
    (i: any) => i.verified && i.value && i.type,
  )
  return hasDisplayName || hasVerifiedIdentity
}

function confirm(question: string): Promise<boolean> {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  return new Promise((resolve) => {
    rl.question(`${question} [y/N] `, (answer) => {
      rl.close()
      resolve(answer.trim().toLowerCase() === 'y')
    })
  })
}

interface AnalysedRow {
  id: string
  data: any
  platform: string
  orgsFixed: number
  orgsDropped: number
}

setImmediate(async () => {
  const dbConnection = await getDbConnection(DB_CONFIG())

  // --- Analysis pass (read-only, offset-based pagination is safe here) ---
  log.info('Analysing rows...')

  const rows: AnalysedRow[] = []
  let totalOrgsFixed = 0
  let totalOrgsDropped = 0
  let offset = 0

  for (;;) {
    const batch = await dbConnection.any<{ id: string; data: any; platform: string }>(
      `SELECT ir.id, ir.data, i.platform
       FROM integration.results ir
       JOIN integrations i ON ir."integrationId" = i.id
       WHERE ir.state = 'error'
         AND (ir.error ->> 'errorMessage') ILIKE $1
       ORDER BY ir."createdAt"
       LIMIT $2 OFFSET $3`,
      [ERROR_PATTERN, BATCH_SIZE, offset],
    )

    if (batch.length === 0) break

    for (const row of batch) {
      const organizations: any[] = row.data?.data?.member?.organizations ?? []
      let orgsFixed = 0
      let orgsDropped = 0

      for (const org of organizations) {
        const fixed = fixOrg(org, row.platform)
        if (isOrgValid(fixed)) {
          orgsFixed++
        } else {
          orgsDropped++
        }
      }

      totalOrgsFixed += orgsFixed
      totalOrgsDropped += orgsDropped
      rows.push({ ...row, orgsFixed, orgsDropped })
    }

    offset += batch.length
    log.info(`  Analysed ${offset} rows so far...`)
  }

  if (rows.length === 0) {
    log.info('No rows matching the error pattern found — nothing to do.')
    process.exit(0)
  }

  log.info(`Analysis complete: ${rows.length} rows found.`)
  log.info(`  Orgs that will be fixed:   ${totalOrgsFixed}`)
  log.info(`  Orgs that will be dropped: ${totalOrgsDropped}`)
  log.info(`  All rows will be updated and set to delayed.`)

  const ok = await confirm('Proceed?')
  if (!ok) {
    log.info('Aborted.')
    process.exit(0)
  }

  // --- Apply pass ---
  let processed = 0

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE)

    for (const row of batch) {
      const data = row.data
      const member = data?.data?.member
      const organizations: any[] = member?.organizations ?? []

      // Fix each org and keep only valid ones
      const fixedOrgs = organizations.map((org) => fixOrg(org, row.platform)).filter(isOrgValid)

      const updatedData = {
        ...data,
        data: {
          ...data.data,
          member: {
            ...member,
            organizations: fixedOrgs,
          },
        },
      }

      await dbConnection.none(
        `UPDATE integration.results
         SET data = $1::jsonb,
             state = 'delayed',
             "delayedUntil" = NOW(),
             error = NULL,
             retries = 0
         WHERE id = $2`,
        [JSON.stringify(updatedData), row.id],
      )
      processed++
    }

    log.info(`  Updated ${processed}/${rows.length} rows`)
  }

  log.info(
    `Done. Updated ${processed} rows (orgs fixed: ${totalOrgsFixed}, orgs dropped: ${totalOrgsDropped}).`,
  )
  process.exit(0)
})
