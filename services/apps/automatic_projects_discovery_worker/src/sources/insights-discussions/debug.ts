/**
 * Debug script: shows which repo URLs are extracted from each discussion.
 * Run with: CROWD_GITHUB_PERSONAL_ACCESS_TOKENS=ghp_... npx ts-node src/sources/insights-discussions/debug.ts
 */
import https from 'https'

const OWNER = 'linuxfoundation'
const REPO = 'insights'
const CATEGORY_SLUG = 'project-onboardings'

interface GraphQLResponse<T> {
  data?: T
  errors?: Array<{ message: string }>
}

async function graphqlRequest<T>(query: string, variables: Record<string, unknown>): Promise<T> {
  const raw = process.env.CROWD_GITHUB_PERSONAL_ACCESS_TOKENS
  if (!raw) throw new Error('CROWD_GITHUB_PERSONAL_ACCESS_TOKENS not set')
  const token = raw.split(',')[0].trim()
  const body = JSON.stringify({ query, variables })

  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: 'api.github.com',
        path: '/graphql',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          Authorization: `Bearer ${token}`,
          'User-Agent': 'crowd-dev-debug',
        },
      },
      (res) => {
        const chunks: Uint8Array[] = []
        res.on('data', (c: Uint8Array) => chunks.push(c))
        res.on('end', () => {
          const response = JSON.parse(Buffer.concat(chunks).toString('utf8')) as GraphQLResponse<T>
          if (response.errors?.length)
            reject(new Error(response.errors.map((e) => e.message).join(', ')))
          else resolve(response.data!)
        })
        res.on('error', reject)
      },
    )
    req.on('error', reject)
    req.write(body)
    req.end()
  })
}

const GITHUB_NON_REPO_OWNERS = new Set(['user-attachments', 'orgs', 'apps', 'marketplace'])

function extractRepoUrls(text: string): string[] {
  const urls = new Set<string>()
  const regex = /https?:\/\/github\.com\/([a-zA-Z0-9_.-]+)\/([a-zA-Z0-9_.-]+)/gi
  let match: RegExpExecArray | null
  while ((match = regex.exec(text)) !== null) {
    const owner = match[1]
    const repo = match[2].replace(/\.git$/, '')
    if (owner && repo && !GITHUB_NON_REPO_OWNERS.has(owner.toLowerCase()))
      urls.add(`https://github.com/${owner}/${repo}`)
  }
  return Array.from(urls)
}

async function main() {
  // Get category ID
  const catData = await graphqlRequest<any>(
    `query { repository(owner: "${OWNER}", name: "${REPO}") { discussionCategories(first: 25) { nodes { id name slug } } } }`,
    {},
  )
  const categories = catData.repository.discussionCategories.nodes
  const category = categories.find((c: any) => c.slug === CATEGORY_SLUG)
  if (!category) throw new Error(`Category ${CATEGORY_SLUG} not found`)
  console.log(`Category: ${category.name} (${category.id})\n`)

  // Fetch all discussions
  let cursor: string | null = null
  let hasNextPage = true
  let totalDiscussions = 0
  const allUrls = new Set<string>()

  while (hasNextPage) {
    const data = await graphqlRequest<any>(
      `query($categoryId: ID!, $cursor: String) {
        repository(owner: "${OWNER}", name: "${REPO}") {
          discussions(first: 100, categoryId: $categoryId, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            nodes { number title body closed }
          }
        }
      }`,
      { categoryId: category.id, cursor },
    )

    const page = data.repository.discussions
    for (const d of page.nodes) {
      totalDiscussions++
      if (d.closed) continue
      const urls = extractRepoUrls(d.body)
      const isMulti = urls.length > 1
      if (isMulti) {
        console.log(`⚠️  Discussion #${d.number}: "${d.title}"`)
        console.log(`   URLs found (${urls.length}):`)
        for (const u of urls) console.log(`     - ${u}`)
        console.log()
      }
      for (const u of urls) allUrls.add(u)
    }

    hasNextPage = page.pageInfo.hasNextPage
    cursor = page.pageInfo.endCursor
  }

  console.log(`---`)
  console.log(`Total discussions: ${totalDiscussions}`)
  console.log(`Total unique repo URLs extracted: ${allUrls.size}`)
}

main().catch(console.error)
