export interface RepoRef {
  owner: string
  name: string
}

export function parseRepoArg(arg: string): RepoRef {
  const path = arg.replace(/^https:\/\/github\.com\//, '').replace(/\/+$/, '')
  const parts = path.split('/')
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new Error(`invalid repo argument "${arg}" - expected owner/repo or a github repo URL`)
  }
  return { owner: parts[0], name: parts[1] }
}
