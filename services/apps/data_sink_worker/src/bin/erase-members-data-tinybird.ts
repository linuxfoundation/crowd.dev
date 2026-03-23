import * as readline from 'readline'

const TINYBIRD_API_URL = 'https://api.us-west-2.aws.tinybird.co/v0/datasources'
const DATA_SOURCES = [
  'activities',
  'activityRelations',
  'members',
  'maintainersInternal',
  'memberIdentities',
]

/**
 * Member Data Erasure Script (Tinybird Analytics Platform)
 *
 * This script removes member data from Tinybird datasources for GDPR compliance
 * and data deletion requests. It complements the database deletion script by
 * cleaning up analytical data stored in Tinybird.
 *
 * WHAT THIS SCRIPT DOES:
 * 1. Shows a detailed summary of records to be deleted from each Tinybird datasource
 * 2. Requests user confirmation before proceeding
 * 3. Deletes data from Tinybird datasources in the correct order to respect dependencies
 * 4. Handles special cases like maintainersInternal which requires identityId-based deletion
 *
 * TINYBIRD INTEGRATION:
 * Uses "Delete Data Selectively" API from Tinybird:
 * https://www.tinybird.co/docs/classic/get-data-in/data-operations/replace-and-delete-data#delete-data-selectively
 *
 * DATASOURCES AFFECTED (in deletion order):
 * 1. activities - Activities records (deleted by activityId retrieved from activityRelations)
 * 2. activityRelations - Activity relationship records (deleted by memberId)
 * 3. maintainersInternal - Repository maintainer records (deleted by identityId from member's identities)
 * 4. memberIdentities - Member identity records (deleted by memberId)
 * 5. members - Member profile data (deleted by memberId)
 *
 * FOREIGN KEY HANDLING:
 * - maintainersInternal.identityId → memberIdentities.id
 *   Solution: Use subquery in delete condition - 'identityId IN (SELECT id FROM memberIdentities WHERE memberId = ?)'
 *
 * DOWNSTREAM EFFECTS:
 * All datasources created from pipes based on these tables will reflect the deletions
 * after the relevant copy pipes run (typically scheduled hourly).
 *
 * USAGE:
 *   npm run script erase-members-data-tinybird <memberId> <tinybirdToken>
 *
 * REQUIREMENTS:
 * - Tinybird token must be provided as command line argument
 * - Token must have delete permissions on the specified datasources
 *
 * SAFETY FEATURES:
 * - Shows detailed deletion summary with record counts before proceeding
 * - Requires explicit user confirmation (Y/n)
 * - Graceful error handling for API failures
 * - Special validation for maintainersInternal dependencies
 */

const args = process.argv.slice(2)

if (args.length !== 2) {
  console.error('Usage: erase-members-data-tinybird.ts <memberId> <tinybirdToken>')
  process.exit(1)
}

const memberId = args[0]
const TOKEN = args[1]

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

/**
 * Gets all identity IDs for a given member from memberIdentities table
 *
 * @param memberId - The member ID to get identity IDs for
 * @returns Array of identity IDs, or empty array if query fails
 */
async function getMemberIdentityIds(memberId: string): Promise<string[]> {
  const query = `SELECT DISTINCT id FROM memberIdentities WHERE memberId = '${memberId}' FORMAT JSON`
  const url = `https://api.us-west-2.aws.tinybird.co/v0/sql`

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      q: query,
    }),
  })

  if (!response.ok) {
    const errorText = await response.text()
    console.warn(
      `Failed to get identity IDs for member ${memberId}: ${response.status} ${response.statusText}`,
    )
    console.warn(`Error response: ${errorText}`)
    return []
  }

  const data = (await response.json()) as { data?: Array<{ id: string }> }
  return data.data?.map((row) => row.id) || []
}

/**
 * Gets all activity IDs for a given member from activityRelations table
 *
 * @param memberId - The member ID to get activity IDs for
 * @returns Array of activity IDs, or empty array if query fails
 */
async function getActivityIds(memberId: string): Promise<string[]> {
  const query = `SELECT DISTINCT activityId FROM activityRelations WHERE memberId = '${memberId}' FORMAT JSON`
  const url = `https://api.us-west-2.aws.tinybird.co/v0/sql`

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      q: query,
    }),
  })

  if (!response.ok) {
    const errorText = await response.text()
    console.warn(
      `Failed to get activity IDs for member ${memberId}: ${response.status} ${response.statusText}`,
    )
    console.warn(`Error response: ${errorText}`)
    return []
  }

  const data = (await response.json()) as { data?: Array<{ activityId: string }> }
  return data.data?.map((row) => row.activityId) || []
}

/**
 * Queries Tinybird to get the count of records matching a condition in a specific datasource
 *
 * @param tableName - The Tinybird datasource name
 * @param condition - SQL WHERE condition to count matching records
 * @returns Number of matching records, or 0 if query fails
 */
async function getRecordCount(tableName: string, condition: string): Promise<number> {
  const query = `SELECT count() as count FROM ${tableName} WHERE ${condition} FORMAT JSON`
  const url = `https://api.us-west-2.aws.tinybird.co/v0/sql`

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      q: query,
    }),
  })

  if (!response.ok) {
    const errorText = await response.text()
    console.warn(`Failed to get count for ${tableName}: ${response.status} ${response.statusText}`)
    console.warn(`Error response: ${errorText}`)
    return 0
  }

  const data = (await response.json()) as { data?: Array<{ count: number }> }
  return data.data?.[0]?.count || 0
}

/**
 * Generates a comprehensive summary of all data that will be deleted from Tinybird
 * datasources for the specified member. Queries each datasource to provide exact record counts.
 *
 * Handles special logic for:
 * - maintainersInternal using a subquery to count records by identityId from the member's identities
 * - activities using activity IDs retrieved from activityRelations
 *
 * @param memberId - The member ID to analyze
 * @returns Formatted summary string showing what will be deleted from each datasource
 */
async function getTinybirdDeletionSummary(
  memberId: string,
  activityIds: string[],
  identityIds: string[],
): Promise<string> {
  let summary = `\n=== TINYBIRD DELETION SUMMARY FOR MEMBER ${memberId} ===\n`

  for (const table of DATA_SOURCES) {
    let condition: string
    let count: number

    if (table === 'activities') {
      // For activities, count based on activity IDs from activityRelations
      if (activityIds.length === 0) {
        count = 0
      } else {
        const activityIdList = activityIds.map((id) => `'${id}'`).join(', ')
        condition = `id IN (${activityIdList})`
        count = await getRecordCount(table, condition)
      }
    } else if (table === 'maintainersInternal') {
      // Count maintainersInternal records using pre-fetched identity IDs
      if (identityIds.length === 0) {
        count = 0
      } else {
        const identityIdList = identityIds.map((id) => `'${id}'`).join(', ')
        condition = `identityId IN (${identityIdList})`
        count = await getRecordCount(table, condition)
      }
    } else if (table === 'members') {
      // Members table uses 'id' as the primary key, not 'memberId'
      condition = `id = '${memberId}'`
      count = await getRecordCount(table, condition)
    } else {
      condition = `memberId = '${memberId}'`
      count = await getRecordCount(table, condition)
    }

    console.log(
      `Checking ${table} with condition: ${condition || 'activities count based on activity IDs'}`,
    )
    console.log(`${table}: ${count} records found (type: ${typeof count})`)

    if (count > 0) {
      summary += `- ${count} records from ${table}\n`
    } else {
      console.log(`No records added to summary for ${table} - count was: ${count}`)
    }
  }

  summary += `\n`
  return summary
}

/**
 * Deletes member data from a specific Tinybird datasource using the appropriate condition.
 *
 * For most datasources, deletes by memberId directly.
 * For maintainersInternal, uses a subquery to delete by identityId from the member's identities.
 * For activities, deletes by activityId using IDs retrieved from activityRelations.
 *
 * @param tableName - The Tinybird datasource name
 * @param memberId - The member ID to delete data for
 * @param activityIds - Optional array of activity IDs (for activities table)
 * @param identityIds - Optional array of identity IDs (for maintainersInternal table)
 */
async function deleteFromDataSource(
  tableName: string,
  memberId: string,
  activityIds?: string[],
  identityIds?: string[],
) {
  const url = `${TINYBIRD_API_URL}/${tableName}/delete`
  let deleteCondition: string

  if (tableName === 'activities') {
    // Delete activities using the provided activity IDs
    if (!activityIds || activityIds.length === 0) {
      console.log(`No activity IDs provided for activities deletion, skipping`)
      return
    }
    const activityIdList = activityIds.map((id) => `'${id}'`).join(', ')
    deleteCondition = `id IN (${activityIdList})`
  } else if (tableName === 'maintainersInternal') {
    // Delete maintainersInternal using pre-fetched identity IDs
    if (!identityIds || identityIds.length === 0) {
      console.log(`No identity IDs found for maintainersInternal deletion, skipping`)
      return
    }
    const identityIdList = identityIds.map((id) => `'${id}'`).join(', ')
    deleteCondition = `identityId IN (${identityIdList})`
  } else if (tableName === 'members') {
    // Members table uses 'id' as the primary key, not 'memberId'
    deleteCondition = `id = '${memberId}'`
  } else {
    deleteCondition = `memberId = '${memberId}'`
  }

  // Safety check: ensure delete condition is not empty and contains expected identifiers
  const skipMemberIdCheck = tableName === 'activities' || tableName === 'maintainersInternal'
  if (!deleteCondition || (!skipMemberIdCheck && !deleteCondition.includes(memberId))) {
    throw new Error(`Invalid delete condition generated: ${deleteCondition}`)
  }

  const body = new URLSearchParams({
    delete_condition: deleteCondition,
  })

  // Log the complete request details before execution
  console.log(`\n=== ABOUT TO DELETE FROM ${tableName.toUpperCase()} ===`)
  console.log(`URL: ${url}`)
  console.log(`Method: POST`)
  console.log(`Headers:`)
  console.log(
    `  Authorization: Bearer ${TOKEN.substring(0, 20)}...${TOKEN.substring(TOKEN.length - 10)}`,
  )
  console.log(`  Content-Type: application/x-www-form-urlencoded`)
  console.log(`Body:`)
  console.log(`  delete_condition: ${deleteCondition}`)
  console.log(`\nEquivalent curl command:`)
  console.log(`curl -X POST \\`)
  console.log(`  -H "Authorization: Bearer ${TOKEN}" \\`)
  console.log(`  -H "Content-Type: application/x-www-form-urlencoded" \\`)
  console.log(`  --data-urlencode 'delete_condition=${deleteCondition}' \\`)
  console.log(`  "${url}"`)

  // Ask for final confirmation for this specific deletion
  const proceed = await promptConfirmation(`\nProceed with deleting from ${tableName}?`)
  if (!proceed) {
    console.log(`Skipped deletion from ${tableName}`)
    return
  }

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body,
  })

  const text = await response.text()
  if (!response.ok) {
    console.error(`Failed to delete from ${tableName}:`, text)
    throw new Error(`Delete failed for table ${tableName}`)
  }

  console.log(`Deleted from ${tableName}:`, text)
}

async function main() {
  // Get activity IDs and identity IDs first (needed for deletion)
  console.log('Getting activity IDs for member...')
  const activityIds = await getActivityIds(memberId)
  console.log(`Found ${activityIds.length} activities to delete`)

  console.log('Getting identity IDs for member...')
  const identityIds = await getMemberIdentityIds(memberId)
  console.log(`Found ${identityIds.length} identities to delete`)

  // Show deletion summary and get confirmation
  const summary = await getTinybirdDeletionSummary(memberId, activityIds, identityIds)
  console.log(summary)

  const proceed = await promptConfirmation('Do you want to proceed with the Tinybird deletion?')

  if (!proceed) {
    console.log('Deletion cancelled by user')
    process.exit(0)
  }

  // Process in order to respect foreign key constraints - activities first, then activityRelations, then maintainersInternal before memberIdentities
  const orderedTables = [
    'activities',
    'activityRelations',
    'maintainersInternal',
    'memberIdentities',
    'members',
  ]

  for (const table of orderedTables) {
    try {
      if (table === 'activities') {
        await deleteFromDataSource(table, memberId, activityIds)
      } else if (table === 'maintainersInternal') {
        await deleteFromDataSource(table, memberId, undefined, identityIds)
      } else {
        await deleteFromDataSource(table, memberId)
      }
    } catch (err) {
      console.error(err)
    }
  }
}

main()
