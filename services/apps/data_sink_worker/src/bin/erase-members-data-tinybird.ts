import * as readline from 'readline'

const TINYBIRD_API = 'https://api.us-west-2.aws.tinybird.co/v0'

// Raw datasources the derived ones are built from; erased first so no copy can rebuild the member
const SOURCE_TABLES = [
  'activities',
  'activityRelations',
  'maintainersInternal',
  'memberIdentities',
  'members',
]

/**
 * Member Data Erasure Script (Tinybird Analytics Platform)
 *
 * This script removes member data from Tinybird datasources for GDPR compliance
 * and data deletion requests. It complements the database deletion script by
 * cleaning up analytical data stored in Tinybird.
 *
 * WHAT THIS SCRIPT DOES:
 * 1. Lists the workspace's datasources and finds every one that holds the member: a `memberId`
 *    column, or a member profile copy (`id`, `attributes` and `joinedAt` columns)
 * 2. Shows how many records each of them holds for the member and asks for confirmation
 * 3. Deletes from the raw sources first, then from every derived datasource, and waits for each
 *    delete job to finish
 * 4. Counts again and deletes once more where rows reappeared (a copy pipe that ran mid-way)
 *
 * TINYBIRD INTEGRATION:
 * Uses "Delete Data Selectively" API from Tinybird:
 * https://www.tinybird.co/docs/classic/get-data-in/data-operations/replace-and-delete-data#delete-data-selectively
 *
 * RAW SOURCES (deleted first, in this order):
 * 1. activities - Activities records (deleted by activityId retrieved from activityRelations)
 * 2. activityRelations - Activity relationship records (deleted by memberId)
 * 3. maintainersInternal - Repository maintainer records (deleted by identityId from member's identities)
 * 4. memberIdentities - Member identity records (deleted by memberId)
 * 5. members - Member profile data (deleted by id)
 *
 * DERIVED DATASOURCES:
 * Served-layer buckets, bucket MVs, cleaned buckets, aggregates and other copies keep their rows
 * after the raw sources are cleaned (copies carry rows forward, MVs never revisit old rows), so
 * every datasource with a `memberId` column is erased too, by memberId. Rows where the member is
 * only the objectMemberId of someone else's activity are kept.
 *
 * FOREIGN KEY HANDLING:
 * - maintainersInternal.identityId → memberIdentities.id
 *   Solution: Use subquery in delete condition - 'identityId IN (SELECT id FROM memberIdentities WHERE memberId = ?)'
 *
 * USAGE:
 *   npm run script erase-members-data-tinybird <memberId> <tinybirdToken>
 *
 * REQUIREMENTS:
 * - Tinybird token must be provided as command line argument
 * - Token must be able to list datasources and delete from them
 *
 * SAFETY FEATURES:
 * - Shows detailed deletion summary with record counts before proceeding
 * - Requires explicit user confirmation (y/N)
 * - Verifies afterwards that no datasource still holds the member
 */

const args = process.argv.slice(2)

if (args.length !== 2) {
  console.error('Usage: erase-members-data-tinybird.ts <memberId> <tinybirdToken>')
  process.exit(1)
}

const memberId = args[0]
const TOKEN = args[1]

if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(memberId)) {
  console.error(`Invalid member id: ${memberId}`)
  process.exit(1)
}

interface Deletion {
  table: string
  condition: string
}

/**
 * Prompts the user for Y/n confirmation via command line input
 */
async function promptConfirmation(message: string): Promise<boolean> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  })

  return new Promise((resolve) => {
    rl.question(`${message} (y/N): `, (answer) => {
      rl.close()
      resolve(answer.toLowerCase() === 'y' || answer.toLowerCase() === 'yes')
    })
  })
}

async function tinybirdRequest(path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(`${TINYBIRD_API}${path}`, {
    ...init,
    headers: { Authorization: `Bearer ${TOKEN}`, ...init.headers },
  })
}

async function runQuery<T>(query: string): Promise<T[]> {
  const response = await tinybirdRequest('/sql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ q: `${query} FORMAT JSON` }),
  })

  if (!response.ok) {
    throw new Error(`Query failed (${response.status}): ${await response.text()}`)
  }

  const data = (await response.json()) as { data?: T[] }
  return data.data || []
}

/**
 * Gets all activity IDs for a given member from activityRelations table
 */
async function getActivityIds(memberId: string): Promise<string[]> {
  const rows = await runQuery<{ activityId: string }>(
    `SELECT DISTINCT activityId FROM activityRelations WHERE memberId = '${memberId}'`,
  )
  return rows.map((row) => row.activityId)
}

// null when the count fails (large tables can hit the Query API time limit)
async function getRecordCount(table: string, condition: string): Promise<number | null> {
  try {
    const rows = await runQuery<{ count: number }>(
      `SELECT count() AS count FROM ${table} WHERE ${condition}`,
    )
    return Number(rows[0]?.count || 0)
  } catch (err) {
    console.warn(`Counting ${table} failed: ${err.message}`)
    return null
  }
}

/**
 * Lists every datasource that holds the member: a memberId column, or a member profile copy
 * (id, attributes and joinedAt columns, like members). Raw sources come first, in SOURCE_TABLES
 * order, so derived copies can't be rebuilt from a source that still has the member.
 */
async function planDeletions(activityIds: string[]): Promise<Deletion[]> {
  const response = await tinybirdRequest('/datasources')
  if (!response.ok) {
    throw new Error(`Listing datasources failed (${response.status}): ${await response.text()}`)
  }

  const { datasources } = (await response.json()) as {
    datasources: { name: string; columns?: { name: string }[] }[]
  }

  const conditions = new Map<string, string>()
  for (const datasource of datasources) {
    const columns = new Set((datasource.columns || []).map((c) => c.name))

    if (datasource.name === 'activities') {
      if (activityIds.length > 0) {
        conditions.set('activities', `id IN (${activityIds.map((id) => `'${id}'`).join(', ')})`)
      }
    } else if (datasource.name === 'maintainersInternal') {
      conditions.set(
        'maintainersInternal',
        `identityId IN (SELECT id FROM memberIdentities WHERE memberId = '${memberId}')`,
      )
    } else if (columns.has('memberId')) {
      conditions.set(datasource.name, `memberId = '${memberId}'`)
    } else if (columns.has('id') && columns.has('attributes') && columns.has('joinedAt')) {
      conditions.set(datasource.name, `id = '${memberId}'`)
    }
  }

  const sources = SOURCE_TABLES.filter((table) => conditions.has(table))
  const derived = [...conditions.keys()].filter((table) => !SOURCE_TABLES.includes(table)).sort()

  return [...sources, ...derived].map((table) => ({ table, condition: conditions.get(table) }))
}

async function waitForJob(jobId: string): Promise<void> {
  for (;;) {
    const response = await tinybirdRequest(`/jobs/${jobId}`)
    if (!response.ok) {
      throw new Error(`Reading job ${jobId} failed (${response.status}): ${await response.text()}`)
    }

    const job = (await response.json()) as { status: string; error?: string }
    if (job.status === 'done') {
      return
    }
    if (job.status === 'error' || job.status === 'cancelled') {
      throw new Error(`Job ${jobId} ended with ${job.status}: ${job.error || ''}`)
    }

    await new Promise((resolve) => setTimeout(resolve, 5000))
  }
}

async function deleteFromDataSource({ table, condition }: Deletion): Promise<void> {
  console.log(`Deleting from ${table} where ${condition}`)

  const response = await tinybirdRequest(`/datasources/${table}/delete`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ delete_condition: condition }),
  })

  const text = await response.text()
  if (!response.ok) {
    throw new Error(`Delete failed for table ${table}: ${text}`)
  }

  const { job_id: jobId } = JSON.parse(text) as { job_id?: string }
  if (jobId) {
    await waitForJob(jobId)
  }
  console.log(`Deleted from ${table}`)
}

async function countRemaining(deletions: Deletion[]): Promise<Map<string, number | null>> {
  const counts = new Map<string, number | null>()
  for (const deletion of deletions) {
    counts.set(deletion.table, await getRecordCount(deletion.table, deletion.condition))
  }
  return counts
}

async function main() {
  console.log('Getting activity IDs for member...')
  const activityIds = await getActivityIds(memberId)
  console.log(`Found ${activityIds.length} activities`)

  const deletions = await planDeletions(activityIds)
  const counts = await countRemaining(deletions)

  console.log(`\n=== TINYBIRD DELETION SUMMARY FOR MEMBER ${memberId} ===`)
  for (const { table, condition } of deletions) {
    const count = counts.get(table)
    console.log(`- ${count ?? 'unknown'} records from ${table} (${condition.slice(0, 80)})`)
  }

  const toDelete = deletions.filter(({ table }) => counts.get(table) !== 0)
  if (toDelete.length === 0) {
    console.log('\nNo Tinybird datasource holds this member')
    return
  }

  const proceed = await promptConfirmation(
    `\nDelete the member from these ${toDelete.length} datasources?`,
  )
  if (!proceed) {
    console.log('Deletion cancelled by user')
    process.exit(0)
  }

  for (const deletion of toDelete) {
    await deleteFromDataSource(deletion)
  }

  const remaining = [...(await countRemaining(deletions))].filter(([, count]) => count > 0)
  for (const [table] of remaining) {
    console.log(`${table} holds the member again, deleting once more`)
    await deleteFromDataSource(deletions.find((deletion) => deletion.table === table))
  }

  const final = await countRemaining(deletions)
  const left = [...final].filter(([, count]) => count > 0)
  const unverified = [...final].filter(([, count]) => count === null).map(([table]) => table)
  for (const [table, count] of left) {
    console.error(`${table} still holds ${count} records of the member`)
  }
  if (unverified.length > 0) {
    console.warn(`Deleted but could not count afterwards: ${unverified.join(', ')}`)
  }
  if (left.length > 0) {
    process.exit(1)
  }

  console.log(`\nMember ${memberId} is gone from every datasource that could be counted`)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
