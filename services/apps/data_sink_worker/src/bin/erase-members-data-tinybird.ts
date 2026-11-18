import * as readline from 'readline'

// GDPR erasure from every Tinybird datasource. Run after erase-member.ts once Kafka Connect has
// caught up, and not between 01:25 and 02:30 UTC while the served-layer merger copies run.

const TINYBIRD_API = 'https://api.us-west-2.aws.tinybird.co/v0'
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
// ClickHouse caps query size (256 KiB by default), so long id lists go in chunks
const IDS_PER_CONDITION = 1000
const JOB_DEADLINE_MS = 60 * 60 * 1000

// Copies with activity content but no id to delete by: rebuilt from their cleaned source instead
const REBUILT_COPIES = [
  { pipe: 'ai_code_tracker_commits_copy', target: 'ai_code_tracker_commits_ds' },
]

// maintainersInternal before memberIdentities, and activityRelations after everything else: a
// rerun after a failure reads the identity and activity ids from them
const SOURCE_TABLES = ['activities', 'maintainersInternal', 'memberIdentities', 'members']

const memberId = process.argv[2]?.toLowerCase()
const TOKEN = process.env.TINYBIRD_TOKEN

if (process.argv.length !== 3 || !TOKEN) {
  console.error(
    'Usage: TINYBIRD_TOKEN=<token> npx tsx src/bin/erase-members-data-tinybird.ts <memberId>',
  )
  process.exit(1)
}

if (!UUID.test(memberId)) {
  console.error(`Invalid member id: ${memberId}`)
  process.exit(1)
}

interface Deletion {
  table: string
  conditions: string[]
}

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

async function selectIds(query: string): Promise<string[]> {
  const ids = (await runQuery<{ id: string }>(query)).map((row) => row.id)
  const invalid = ids.find((id) => !UUID.test(id))
  if (invalid) {
    throw new Error(`Unexpected id ${invalid} from: ${query}`)
  }
  return ids
}

function idConditions(column: string, ids: string[]): string[] {
  const conditions: string[] = []
  for (let i = 0; i < ids.length; i += IDS_PER_CONDITION) {
    const chunk = ids.slice(i, i + IDS_PER_CONDITION)
    conditions.push(`${column} IN (${chunk.map((id) => `'${id}'`).join(', ')})`)
  }
  return conditions
}

// null when a count fails (large tables can hit the Query API time limit)
async function countRecords({ table, conditions }: Deletion): Promise<number | null> {
  let total = 0
  for (const condition of conditions) {
    try {
      const rows = await runQuery<{ count: number }>(
        `SELECT count() AS count FROM ${table} WHERE ${condition}`,
      )
      total += Number(rows[0]?.count || 0)
    } catch (err) {
      console.warn(`Counting ${table} failed: ${(err as Error).message}`)
      return null
    }
  }
  return total
}

async function planDeletions(activityIds: string[], identityIds: string[]): Promise<Deletion[]> {
  const response = await tinybirdRequest('/datasources')
  if (!response.ok) {
    throw new Error(`Listing datasources failed (${response.status}): ${await response.text()}`)
  }

  const { datasources } = (await response.json()) as {
    datasources: { name: string; columns?: { name: string }[] }[]
  }

  const byMember = `memberId = '${memberId}'`
  const plan = new Map<string, string[]>()
  const activityTables: string[] = []

  for (const { name, columns } of datasources) {
    // shared from another workspace: not ours to delete from
    if (name.includes('.')) {
      continue
    }

    const names = new Set((columns || []).map((c) => c.name))
    if (names.has('id') && names.has('sourceId') && names.has('body')) {
      activityTables.push(name)
      const conditions = idConditions('id', activityIds)
      plan.set(name, names.has('memberId') ? [...conditions, byMember] : conditions)
    } else if (name === 'maintainersInternal') {
      plan.set(name, idConditions('identityId', identityIds))
    } else if (names.has('memberId')) {
      plan.set(name, [byMember])
    } else if (names.has('id') && names.has('attributes') && names.has('joinedAt')) {
      plan.set(name, [`id = '${memberId}'`])
    }
  }

  // activity copies only derive from activities, so they go with the raw sources
  const first = [
    'activities',
    ...activityTables.filter((table) => table !== 'activities').sort(),
    ...SOURCE_TABLES.filter((table) => table !== 'activities'),
  ]
  const derived = [...plan.keys()]
    .filter((table) => !first.includes(table) && table !== 'activityRelations')
    .sort()
  const order = [...first, ...derived, 'activityRelations']

  return order
    .filter((table) => (plan.get(table) || []).length > 0)
    .map((table) => ({ table, conditions: plan.get(table) as string[] }))
}

async function waitForJob(jobId: string, table: string): Promise<void> {
  const started = Date.now()
  let lastLog = started

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
      throw new Error(
        `Delete job ${jobId} on ${table} ended with ${job.status}: ${job.error || ''}`,
      )
    }
    if (Date.now() - started > JOB_DEADLINE_MS) {
      throw new Error(`Delete job ${jobId} on ${table} still ${job.status} after 60 minutes`)
    }
    if (Date.now() - lastLog > 60_000) {
      console.log(`  ${table}: job ${jobId} ${job.status}`)
      lastLog = Date.now()
    }

    await new Promise((resolve) => setTimeout(resolve, 5000))
  }
}

async function deleteFrom({ table, conditions }: Deletion): Promise<void> {
  for (const condition of conditions) {
    console.log(`Deleting from ${table} where ${condition.slice(0, 100)}`)

    const response = await tinybirdRequest(`/datasources/${encodeURIComponent(table)}/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ delete_condition: condition }),
    })

    const text = await response.text()
    if (!response.ok) {
      throw new Error(`Delete failed for table ${table}: ${text}`)
    }

    const { job_id: jobId } = JSON.parse(text) as { job_id?: string }
    if (!jobId) {
      throw new Error(`Delete on ${table} returned no job: ${text}`)
    }
    await waitForJob(jobId, table)
  }
  console.log(`Deleted from ${table}`)
}

async function rebuildCopy({ pipe, target }: { pipe: string; target: string }): Promise<void> {
  console.log(`Rebuilding ${target} with ${pipe}`)
  const response = await tinybirdRequest(`/pipes/${encodeURIComponent(pipe)}/copy`, {
    method: 'POST',
  })

  const text = await response.text()
  if (!response.ok) {
    throw new Error(`Running ${pipe} failed: ${text}`)
  }

  const { job_id: jobId, id } = JSON.parse(text) as { job_id?: string; id?: string }
  if (!jobId && !id) {
    throw new Error(`Running ${pipe} returned no job: ${text}`)
  }
  await waitForJob(jobId || id, target)
  console.log(`Rebuilt ${target}`)
}

async function countAll(deletions: Deletion[]): Promise<Map<string, number | null>> {
  const counts = new Map<string, number | null>()
  for (const deletion of deletions) {
    counts.set(deletion.table, await countRecords(deletion))
  }
  return counts
}

// Only activities whose newest relation version still belongs to the member: an older version of
// an activity a merge moved to another member must not take that member's activity along
async function currentActivityIds(): Promise<string[]> {
  const rows = await runQuery<{ activityId: string; segmentId: string; timestamp: string }>(
    `SELECT DISTINCT activityId, segmentId, toString(timestamp) AS timestamp
     FROM activityRelations WHERE memberId = '${memberId}'`,
  )
  const latest = new Map<string, { memberId: string; updatedAt: string }>()

  for (let i = 0; i < rows.length; i += IDS_PER_CONDITION) {
    const chunk = rows.slice(i, i + IDS_PER_CONDITION)
    for (const row of chunk) {
      if (!UUID.test(row.activityId) || !UUID.test(row.segmentId)) {
        throw new Error(`Unexpected relation ${JSON.stringify(row)}`)
      }
    }
    const segments = [...new Set(chunk.map((row) => `'${row.segmentId}'`))].join(', ')
    const timestamps = [...new Set(chunk.map((row) => `'${row.timestamp}'`))].join(', ')
    const ids = chunk.map((row) => `'${row.activityId}'`).join(', ')

    const versions = await runQuery<{ activityId: string; memberId: string; updatedAt: string }>(
      `SELECT activityId, memberId, toString(updatedAt) AS updatedAt FROM activityRelations
       WHERE segmentId IN (${segments}) AND timestamp IN (${timestamps}) AND activityId IN (${ids})`,
    )
    for (const version of versions) {
      const current = latest.get(version.activityId)
      if (!current || version.updatedAt > current.updatedAt) {
        latest.set(version.activityId, version)
      }
    }
  }

  return [...latest].filter(([, version]) => version.memberId === memberId).map(([id]) => id)
}

async function main() {
  const activityIds = await currentActivityIds()
  const identityIds = await selectIds(
    `SELECT DISTINCT id FROM memberIdentities WHERE memberId = '${memberId}'`,
  )
  console.log(`Found ${activityIds.length} activities and ${identityIds.length} identities`)

  const deletions = await planDeletions(activityIds, identityIds)
  const counts = await countAll(deletions)

  console.log(`\n=== TINYBIRD DELETION SUMMARY FOR MEMBER ${memberId} ===`)
  for (const { table } of deletions) {
    console.log(`- ${counts.get(table) ?? 'unknown'} records from ${table}`)
  }

  for (const { target, pipe } of REBUILT_COPIES) {
    console.log(`- ${target} is rebuilt with ${pipe} afterwards (it has no key to count by)`)
  }

  const toDelete = deletions.filter(({ table }) => counts.get(table) !== 0)
  const proceed = await promptConfirmation(
    toDelete.length > 0
      ? `\nDelete the member from these ${toDelete.length} datasources and rebuild the copies?`
      : '\nNo datasource holds this member. Rebuild the copies anyway?',
  )
  if (!proceed) {
    console.log('Deletion cancelled by user')
    process.exit(3)
  }

  for (const deletion of toDelete) {
    await deleteFrom(deletion)
  }

  const afterFirstPass = await countAll(deletions)
  for (const deletion of deletions.filter(({ table }) => afterFirstPass.get(table) !== 0)) {
    console.log(
      `${deletion.table}: deleting once more (count ${afterFirstPass.get(deletion.table) ?? 'unknown'})`,
    )
    await deleteFrom(deletion)
  }

  for (const copy of REBUILT_COPIES) {
    await rebuildCopy(copy)
  }

  const final = await countAll(deletions)
  const left = [...final].filter(([, count]) => count !== null && count > 0)
  const unverified = [...final].filter(([, count]) => count === null).map(([table]) => table)

  for (const [table, count] of left) {
    console.error(`${table} still holds ${count} records of the member`)
  }
  if (left.length > 0) {
    process.exit(1)
  }
  if (unverified.length > 0) {
    console.warn(`Deleted twice but could not count afterwards: ${unverified.join(', ')}`)
    process.exit(2)
  }

  console.log(
    `\nMember ${memberId} is gone from every counted Tinybird datasource, and ${REBUILT_COPIES.map(
      ({ target }) => target,
    ).join(', ')} was rebuilt from them`,
  )
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
