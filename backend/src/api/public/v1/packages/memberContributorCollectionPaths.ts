import type { MemberContributorCollectionPath } from '@crowd/data-access-layer'

export const MEMBER_CONTRIBUTOR_COLLECTION_PATHS: Record<string, MemberContributorCollectionPath> =
  {
    npm: 'github',
    maven: 'github',
    pypi: 'github',
    cargo: 'github',
    golang: 'github',
    nuget: 'github',
    composer: 'github',
    gem: 'github',
  }

export function purlType(purl: string): string {
  const afterScheme = purl.slice('pkg:'.length)
  const slash = afterScheme.indexOf('/')
  const type = slash === -1 ? afterScheme : afterScheme.slice(0, slash)
  return type.toLowerCase()
}

export function canonicalizePurlType(purl: string): string {
  const schemeLength = 'pkg:'.length
  const slash = purl.indexOf('/', schemeLength)
  const typeEnd = slash === -1 ? purl.length : slash
  return (
    purl.slice(0, schemeLength) +
    purl.slice(schemeLength, typeEnd).toLowerCase() +
    purl.slice(typeEnd)
  )
}

const WELL_FORMED_PURL = /^pkg:[a-z][a-z0-9.+-]*\/[^/?#]+/i

function isDecodable(value: string): boolean {
  try {
    decodeURIComponent(value)
    return true
  } catch {
    return false
  }
}

export function isWellFormedPurl(purl: string): boolean {
  return WELL_FORMED_PURL.test(purl) && isDecodable(purl.slice('pkg:'.length))
}

export const LLM_REPO_RESOLUTION_PATH: MemberContributorCollectionPath = 'llm_repo_resolution'

export function collectionPathForPurlType(type: string): MemberContributorCollectionPath {
  return Object.hasOwn(MEMBER_CONTRIBUTOR_COLLECTION_PATHS, type)
    ? MEMBER_CONTRIBUTOR_COLLECTION_PATHS[type]
    : LLM_REPO_RESOLUTION_PATH
}
