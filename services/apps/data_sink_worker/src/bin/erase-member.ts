import * as readline from 'readline'

import { SearchSyncWorkerEmitter } from '@crowd/common_services'
import { DbStore, getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceChildLogger } from '@crowd/logging'
import { QueueFactory } from '@crowd/queue'

import { DB_CONFIG, QUEUE_CONFIG } from '../conf'

/**
 * Member Data Erasure Script (Database)
 *
 * This script completely removes a member and all associated data from the database
 * for GDPR compliance and data deletion requests. It performs a comprehensive cleanup
 * across multiple related tables while respecting foreign key constraints.
 *
 * WHAT THIS SCRIPT DOES:
 * 1. Shows a detailed summary of all data to be deleted/modified
 * 2. Requests user confirmation before proceeding
 * 3. Performs the following operations in order:
 *    - Archives member identities to requestedForErasureMemberIdentities (separate step)
 *    - Deletes from maintainersInternal (respects FK constraint with memberIdentities)
 *    - Deletes from all member-related tables (relations, segments, etc.)
 *    - Deletes the main member record
 *    - Triggers search index updates and organization re-sync
 *
 * FOREIGN KEY HANDLING:
 * - maintainersInternal.identityId → memberIdentities.id
 *   Solution: Delete maintainersInternal records first before memberIdentities
 *
 * TABLES AFFECTED:
 * - maintainersInternal (deleted by identityId from member's identities)
 * - requestedForErasureMemberIdentities (memberIdentities are inserted here before deletion)
 * - activityRelations, memberNoMerge, memberOrganizationAffiliationOverrides
 * - memberOrganizations, memberSegmentAffiliations, memberSegments, memberSegmentsAgg
 * - memberEnrichmentCache, memberEnrichments, memberIdentities
 * - memberToMerge, memberToMergeRaw, memberBotSuggestions, memberNoBot
 * - members (main record)
 *
 * SEARCH INDEX UPDATES:
 * - Removes member from search indexes
 * - Re-syncs any affected organizations
 *
 * USAGE:
 *   npm run script erase-member <memberId>
 *
 * SAFETY FEATURES:
 * - Shows detailed deletion summary before proceeding
 * - Requires explicit user confirmation (Y/n)
 * - Runs in a database transaction for atomicity
 * - Comprehensive error handling and logging
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

const log = getServiceChildLogger('erase-member')

/**
 * Prompts the user for Y/n confirmation via command line input
 */
async function promptConfirmation(message: string): Promise<boolean> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  })

  return new Promise((resolve) => {
    rl.question(`${message} (Y/n): `, (answer) => {
      rl.close()
      resolve(answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes' || answer === '')
    })
  })
}

const processArguments = process.argv.slice(2)

if (processArguments.length !== 1) {
  log.error('Expected exactly one argument: memberId')
  process.exit(1)
}

const memberId = processArguments[0]

setImmediate(async () => {
  const dbConnection = await getDbConnection(DB_CONFIG())
  const store = new DbStore(log, dbConnection)
  const queueClient = QueueFactory.createQueueService(QUEUE_CONFIG())

  const searchSyncWorkerEmitter = new SearchSyncWorkerEmitter(queueClient, log)
  await searchSyncWorkerEmitter.init()

  log.info(`Erasing member with ID: ${memberId}`)

  const orgDataMap: Map<string, any[]> = new Map()
  const memberDataMap: Map<string, any> = new Map()

  try {
    // Show deletion summary and get confirmation
    const summary = await getDeletionSummary(store, memberId)
    console.log(summary)

    const proceed = await promptConfirmation('Do you want to proceed with the deletion?')

    if (!proceed) {
      log.info('Deletion cancelled by user')
      process.exit(0)
    }

    await store.transactionally(async (t) => {
      // get organization id for a member to sync later
      let orgResults: any[]
      if (orgDataMap.has(memberId)) {
        orgResults = orgDataMap.get(memberId)
      } else {
        orgResults = await store
          .connection()
          .any(
            `select distinct "organizationId" from "activityRelations" where "memberId" = $(memberId)`,
            {
              memberId,
            },
          )
        orgDataMap.set(memberId, orgResults)
      }

      let memberData: any
      if (memberDataMap.has(memberId)) {
        memberData = memberDataMap.get(memberId)
      } else {
        memberData = await store.connection().one(`select * from members where id = $(memberId)`, {
          memberId,
        })
        memberDataMap.set(memberId, memberData)
      }

      log.info('CLEANUP ACTIVITIES...')

      // Archive member identities before deletion
      await archiveMemberIdentities(t, memberId)

      // delete the member and everything around it
      await deleteMemberFromDb(t, memberId)

      await searchSyncWorkerEmitter.triggerRemoveMember(memberId, true)

      if (orgResults.length > 0) {
        for (const orgResult of orgResults) {
          if (orgResult.organizationId) {
            await searchSyncWorkerEmitter.triggerOrganizationSync(orgResult.organizationId, true)
          }
        }
      }
    })
  } catch (err) {
    log.error(err, { memberId }, 'Failed to erase member!')
  }

  process.exit(0)
})

/**
 * Generates a comprehensive summary of all data that will be deleted or modified
 * for the specified member. Queries each table to provide exact record counts.
 *
 * @param store - Database store instance
 * @param memberId - The member ID to analyze
 * @returns Formatted summary string showing what will be affected
 */
async function getDeletionSummary(store: DbStore, memberId: string): Promise<string> {
  let summary = `\n=== DELETION SUMMARY FOR MEMBER ${memberId} ===\n`

  // Count activities that will be updated (objectMemberId set to null)
  const activityRelationsUpdate = await store
    .connection()
    .one(`select count(*) as count from "activityRelations" where "objectMemberId" = $(memberId)`, {
      memberId,
    })
  if (parseInt(activityRelationsUpdate.count) > 0) {
    summary += `- ${activityRelationsUpdate.count} activityRelations will have objectMemberId/objectMemberUsername cleared\n`
  }

  // Count maintainersInternal records to be deleted
  const maintainersCount = await store.connection().one(
    `select count(*) as count from "maintainersInternal" where "identityId" in (
      select id from "memberIdentities" where "memberId" = $(memberId)
    )`,
    { memberId },
  )
  if (parseInt(maintainersCount.count) > 0) {
    summary += `- ${maintainersCount.count} maintainersInternal records will be deleted\n`
  }

  // Count records in each table to be deleted
  const tablesToDelete: Map<string, string[]> = new Map([
    ['activityRelations', ['memberId']],
    ['memberNoMerge', ['memberId', 'noMergeId']],
    ['memberOrganizationAffiliationOverrides', ['memberId']],
    ['memberOrganizations', ['memberId']],
    ['memberSegmentAffiliations', ['memberId']],
    ['memberSegments', ['memberId']],
    ['memberSegmentsAgg', ['memberId']],
    ['memberEnrichmentCache', ['memberId']],
    ['memberEnrichments', ['memberId']],
    ['memberIdentities', ['memberId']],
    ['memberToMerge', ['memberId', 'toMergeId']],
    ['memberToMergeRaw', ['memberId', 'toMergeId']],
    ['memberBotSuggestions', ['memberId']],
    ['memberNoBot', ['memberId']],
  ])

  for (const [table, memberIdColumns] of tablesToDelete) {
    const condition = memberIdColumns.map((c) => `"${c}" = $(memberId)`).join(' or ')
    const result = await store
      .connection()
      .one(`select count(*) as count from "${table}" where ${condition}`, { memberId })
    if (parseInt(result.count) > 0) {
      if (table === 'memberIdentities') {
        summary += `- ${result.count} records from ${table} (will be inserted into requestedForErasureMemberIdentities first)\n`
      } else {
        summary += `- ${result.count} records from ${table}\n`
      }
    }
  }

  // Count main member record
  const memberExists = await store
    .connection()
    .one(`select count(*) as count from members where id = $(memberId)`, { memberId })
  if (parseInt(memberExists.count) > 0) {
    summary += `- 1 member record\n`
  }

  summary += `\n`
  return summary
}

/**
 * Archives member identities to requestedForErasureMemberIdentities table before deletion.
 * This preserves identity data for audit/compliance purposes while allowing for GDPR deletion.
 *
 * @param store - Database store instance (should be within a transaction)
 * @param memberId - The member ID whose identities will be archived
 * @returns Number of identities archived
 */
export async function archiveMemberIdentities(store: DbStore, memberId: string): Promise<number> {
  const insertResult = await store.connection().result(
    `
    INSERT INTO "requestedForErasureMemberIdentities" (
      id, platform, value, type
    )
    SELECT id, platform, value, type
    FROM "memberIdentities" 
    WHERE "memberId" = $(memberId)
    `,
    { memberId },
  )

  if (insertResult.rowCount > 0) {
    log.info(
      `Archived ${insertResult.rowCount} memberIdentities to requestedForErasureMemberIdentities for member ${memberId}`,
    )
  }

  return insertResult.rowCount
}

/**
 * Performs the actual deletion of a member and all associated data from the database.
 * This function handles the complex deletion order required by foreign key constraints.
 *
 * DELETION ORDER:
 * 1. Clear activityRelations.objectMemberId references (update, not delete)
 * 2. Delete maintainersInternal records (by identityId from memberIdentities)
 * 3. Delete from all member-related tables (including memberIdentities)
 * 4. Delete the main member record
 *
 * @param store - Database store instance (should be within a transaction)
 * @param memberId - The member ID to delete
 */
export async function deleteMemberFromDb(store: DbStore, memberId: string): Promise<void> {
  let result = await store.connection().result(
    `
    update "activityRelations" set
      "objectMemberId" = null,
      "objectMemberUsername" = null,
      "updatedAt" = now()
    where "objectMemberId" is not null and "objectMemberId" = $(memberId)
    `,
    {
      memberId,
    },
  )

  if (result.rowCount > 0) {
    log.info(
      `Cleared ${result.rowCount} activityRelations."objectMemberId" and activityRelations."objectMemberUsername" for memberId ${memberId}!`,
    )
  }

  // Delete from maintainersInternal first (foreign key constraint with memberIdentities.id)
  const maintainersQuery = `delete from "maintainersInternal" where "identityId" in (
      select id from "memberIdentities" where "memberId" = '${memberId}'
    )`
  console.log(`\n=== ABOUT TO DELETE FROM MAINTAINERSINTERNAL ===`)
  console.log(`Query: ${maintainersQuery}`)
  const proceedMaintainers = await promptConfirmation(
    'Proceed with deleting from maintainersInternal?',
  )
  if (!proceedMaintainers) {
    throw new Error('User cancelled deletion from maintainersInternal')
  }

  result = await store.connection().result(
    `delete from "maintainersInternal" where "identityId" in (
      select id from "memberIdentities" where "memberId" = $(memberId)
    )`,
    { memberId },
  )

  if (result.rowCount > 0) {
    log.info(
      `Deleted ${result.rowCount} rows from table maintainersInternal for member ${memberId}!`,
    )
  }

  const tablesToDelete: Map<string, string[]> = new Map([
    ['activityRelations', ['memberId']],
    ['memberNoMerge', ['memberId', 'noMergeId']],
    ['memberOrganizationAffiliationOverrides', ['memberId']],
    ['memberOrganizations', ['memberId']],
    ['memberSegmentAffiliations', ['memberId']],
    ['memberSegments', ['memberId']],
    ['memberSegmentsAgg', ['memberId']],
    ['memberEnrichmentCache', ['memberId']],
    ['memberEnrichments', ['memberId']],
    ['memberIdentities', ['memberId']],
    ['memberToMerge', ['memberId', 'toMergeId']],
    ['memberToMergeRaw', ['memberId', 'toMergeId']],
    ['memberBotSuggestions', ['memberId']],
    ['memberNoBot', ['memberId']],
  ])

  for (const table of Array.from(tablesToDelete.keys())) {
    const memberIdColumns = tablesToDelete.get(table)
    log.warn(`Deleting member ${memberId} from table ${table} by columns ${memberIdColumns}...`)

    if (memberIdColumns.length === 0) {
      throw new Error(`No fk columns specified for table ${table}!`)
    }

    const condition = memberIdColumns.map((c) => `"${c}" = $(memberId)`).join(' or ')
    const deleteQuery = `delete from "${table}" where ${condition.replace('$(memberId)', `'${memberId}'`)}`
    console.log(`\n=== ABOUT TO DELETE FROM ${table.toUpperCase()} ===`)
    console.log(`Query: ${deleteQuery}`)
    const proceedTable = await promptConfirmation(`Proceed with deleting from ${table}?`)
    if (!proceedTable) {
      log.info(`Skipped deletion from ${table}`)
      continue
    }

    result = await store
      .connection()
      .result(`delete from "${table}" where ${condition}`, { memberId })

    if (result.rowCount > 0) {
      log.info(`Deleted ${result.rowCount} rows from table ${table} for member ${memberId}!`)
    }
  }

  const finalDeleteQuery = `delete from members where id = '${memberId}'`
  console.log(`\n=== ABOUT TO DELETE MAIN MEMBER RECORD ===`)
  console.log(`Query: ${finalDeleteQuery}`)
  const proceedFinal = await promptConfirmation('Proceed with deleting the main member record?')
  if (!proceedFinal) {
    throw new Error('User cancelled deletion of main member record')
  }

  result = await store
    .connection()
    .result(`delete from members where id = $(memberId)`, { memberId })

  if (result.rowCount === 0) {
    throw new Error(`Failed to delete member - memberId ${memberId}!`)
  }
}
