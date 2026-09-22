import { ExtractorDeps } from '../types'

/** null means unresolved (empty repo, truncated, fetch failure) — callers must fall back to probing. */
export interface RepoTree {
  paths: Set<string> | null
}

interface GitTreeEntry {
  path?: unknown
  type?: unknown
}

interface GitTreeResponse {
  tree?: GitTreeEntry[]
  truncated?: boolean
}

// `HEAD` resolves to the default branch on the git data API.
export async function fetchRepoTree(
  owner: string,
  name: string,
  githubGet: ExtractorDeps['githubGet'],
): Promise<RepoTree> {
  try {
    const { text } = await githubGet(`/repos/${owner}/${name}/git/trees/HEAD?recursive=1`)
    if (!text) return { paths: null }

    const doc = JSON.parse(text) as GitTreeResponse
    if (doc.truncated || !Array.isArray(doc.tree)) return { paths: null }

    const paths = new Set<string>()
    for (const entry of doc.tree) {
      if (entry.type === 'blob' && typeof entry.path === 'string') paths.add(entry.path)
    }
    return { paths }
  } catch {
    return { paths: null }
  }
}
