import axios, { AxiosError } from 'axios'
import commandLineArgs from 'command-line-args'

import { DB_CONFIG } from '@/conf'
import { getGithubInstallationToken } from '@crowd/common_services'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

const NOT_FOUND_STATUSES = [404, 409, 422, 451]

type LookupResult = 'linked' | 'no-github-account' | 'commit-not-found' | 'not-on-github'

interface CommitOnlyMember {
  memberId: string
  email: string
  channel: string | null
  sha: string | null
}

async function fetchCommitOnlyMembersSample(
  qx: QueryExecutor,
  limit: number,
): Promise<CommitOnlyMember[]> {
  return qx.select(
    `
      WITH candidates AS (
        SELECT DISTINCT ON (mi."memberId") mi."memberId", mi.value AS email
        FROM "memberIdentities" mi TABLESAMPLE SYSTEM (1)
        WHERE mi.platform = 'git'
          AND mi.type = 'username'
          AND mi.verified = true
          AND mi."deletedAt" IS NULL
          AND lower(mi.value) NOT LIKE '%@users.noreply.github.com'
          AND NOT EXISTS (
            SELECT 1
            FROM "memberIdentities" other
            WHERE other."memberId" = mi."memberId"
              AND other.platform <> 'git'
              AND other."deletedAt" IS NULL
          )
      ),
      sample AS (
        SELECT * FROM candidates ORDER BY random() LIMIT $(limit)
      )
      SELECT s."memberId", s.email, commit.channel, commit."sourceId" AS sha
      FROM sample s
      LEFT JOIN LATERAL (
        SELECT ar.channel, ar."sourceId"
        FROM "activityRelations" ar
        WHERE ar."memberId" = s."memberId"
          AND ar.platform = 'git'
          AND ar.type = 'authored-commit'
          AND ar.channel LIKE 'https://github.com/%'
        LIMIT 1
      ) commit ON true
    `,
    { limit },
  )
}

async function findGithubUsernameOwners(
  qx: QueryExecutor,
  usernames: string[],
): Promise<Map<string, string>> {
  if (usernames.length === 0) {
    return new Map()
  }

  const rows: { username: string; memberId: string }[] = await qx.select(
    `
      SELECT lower(value) AS username, "memberId"
      FROM "memberIdentities"
      WHERE platform = 'github'
        AND type = 'username'
        AND verified = true
        AND "deletedAt" IS NULL
        AND lower(value) IN ($(usernames:csv))
    `,
    { usernames: usernames.map((username) => username.toLowerCase()) },
  )

  return new Map(rows.map(({ username, memberId }) => [username, memberId]))
}

async function fetchCommitAuthorLogin(
  channel: string,
  sha: string,
): Promise<{ result: LookupResult; githubLogin: string | null }> {
  const [owner, repo] = new URL(channel).pathname
    .replace(/\.git$/, '')
    .split('/')
    .filter(Boolean)
  const token = await getGithubInstallationToken()

  try {
    const { data } = await axios.get(
      `https://api.github.com/repos/${owner}/${repo}/commits/${sha}`,
      {
        headers: {
          Accept: 'application/vnd.github+json',
          Authorization: `Bearer ${token}`,
          'X-GitHub-Api-Version': '2022-11-28',
        },
        timeout: 15_000,
      },
    )

    const githubLogin: string | null = data.author?.login ?? null
    return { result: githubLogin ? 'linked' : 'no-github-account', githubLogin }
  } catch (err) {
    if (err instanceof AxiosError && NOT_FOUND_STATUSES.includes(err.response?.status)) {
      return { result: 'commit-not-found', githubLogin: null }
    }

    throw err
  }
}

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (sample 10 members instead of 1000).',
  },
]

const parameters = commandLineArgs(options)

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const SAMPLE_SIZE = testRun ? 10 : 1000

  const db = await getDbConnection({
    host: DB_CONFIG.readHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)

  log.info({ testRun, SAMPLE_SIZE }, 'Running script with the following parameters!')

  const members = await fetchCommitOnlyMembersSample(qx, SAMPLE_SIZE)
  const lookups: (CommitOnlyMember & { result: LookupResult; githubLogin: string | null })[] = []

  for (const chunk of chunkArray(members, 10)) {
    const chunkLookups = await Promise.all(
      chunk.map(async (member) => {
        if (!member.channel || !member.sha) {
          return { ...member, result: 'not-on-github' as const, githubLogin: null }
        }

        return { ...member, ...(await fetchCommitAuthorLogin(member.channel, member.sha)) }
      }),
    )

    lookups.push(...chunkLookups)
  }

  const owners = await findGithubUsernameOwners(
    qx,
    lookups.filter(({ githubLogin }) => githubLogin).map(({ githubLogin }) => githubLogin),
  )

  const results = lookups.map((lookup) => ({
    ...lookup,
    ownerId: owners.get(lookup.githubLogin?.toLowerCase()) ?? null,
  }))

  for (const result of results) {
    log.info(result, 'Looked up commit author!')
  }

  const countOf = (result: LookupResult) => results.filter((r) => r.result === result).length
  const linked = countOf('linked')
  const linkedToExistingMember = results.filter(({ ownerId }) => ownerId).length

  log.info(
    {
      sampled: results.length,
      linked,
      linkedToExistingMember,
      linkedToNewIdentity: linked - linkedToExistingMember,
      noGithubAccount: countOf('no-github-account'),
      commitNotFound: countOf('commit-not-found'),
      notOnGithub: countOf('not-on-github'),
      linkRate: results.length ? Number((linked / results.length).toFixed(3)) : 0,
    },
    'Pilot summary!',
  )

  process.exit(0)
})
