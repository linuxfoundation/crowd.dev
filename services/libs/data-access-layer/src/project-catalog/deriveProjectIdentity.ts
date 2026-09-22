export function deriveProjectIdentityFromRepoUrl(
  repoUrl: string,
): { projectSlug: string; repoName: string } | null {
  let urlPath: string
  try {
    urlPath = new URL(repoUrl).pathname.replace(/^\//, '').replace(/\/$/, '')
  } catch {
    urlPath = repoUrl.replace(/\/$/, '').split('/').slice(-2).join('/')
  }

  const repoName = urlPath.split('/').pop() || ''
  if (!urlPath || !repoName) {
    return null
  }

  return { projectSlug: urlPath, repoName }
}
