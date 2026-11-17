import { DEFAULT_TENANT_ID } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import type { SegmentDbRow } from '@crowd/types'

const NON_LF_SLUG_PREFIX = 'nonlf_'

export type ExistingProjectMatch = 'slug' | 'name'

export type CdpSubproject = Pick<SegmentDbRow, 'id' | 'slug' | 'name' | 'sourceId' | 'isLF'>

export interface ExistingProjectCandidate extends CdpSubproject {
  matchedOn: ExistingProjectMatch[]
}

export interface CdpSubprojectIndex {
  bySlug: Map<string, CdpSubproject[]>
  byName: Map<string, CdpSubproject[]>
}

function slugKey(slug: string): string {
  const lowered = slug.trim().toLowerCase()
  return lowered.startsWith(NON_LF_SLUG_PREFIX) ? lowered.slice(NON_LF_SLUG_PREFIX.length) : lowered
}

function nameKey(name: string): string {
  return name.trim().toLowerCase()
}

function addToIndex(
  index: Map<string, CdpSubproject[]>,
  key: string,
  subproject: CdpSubproject,
): void {
  if (!key) return
  const subprojects = index.get(key)
  if (subprojects) subprojects.push(subproject)
  else index.set(key, [subproject])
}

export function buildCdpSubprojectIndex(subprojects: CdpSubproject[]): CdpSubprojectIndex {
  const index: CdpSubprojectIndex = { bySlug: new Map(), byName: new Map() }
  for (const subproject of subprojects) {
    addToIndex(index.bySlug, slugKey(subproject.slug), subproject)
    addToIndex(index.byName, nameKey(subproject.name ?? ''), subproject)
  }
  return index
}

export function findExistingProjectCandidates(
  index: CdpSubprojectIndex,
  pccSlug: string | null,
  pccName: string,
): ExistingProjectCandidate[] {
  const matches = new Map<string, ExistingProjectCandidate>()
  const collect = (subprojects: CdpSubproject[] | undefined, matchedOn: ExistingProjectMatch) => {
    for (const subproject of subprojects ?? []) {
      const candidate = matches.get(subproject.id)
      if (candidate) candidate.matchedOn.push(matchedOn)
      else matches.set(subproject.id, { ...subproject, matchedOn: [matchedOn] })
    }
  }

  if (pccSlug) collect(index.bySlug.get(slugKey(pccSlug)), 'slug')
  collect(index.byName.get(nameKey(pccName)), 'name')

  return [...matches.values()]
}

export async function loadCdpSubprojectIndex(qx: QueryExecutor): Promise<CdpSubprojectIndex> {
  const subprojects: CdpSubproject[] = await qx.select(
    `
    SELECT id, slug, name, "sourceId", "isLF"
    FROM segments
    WHERE type = 'subproject'
      AND "tenantId" = $(tenantId)
    `,
    { tenantId: DEFAULT_TENANT_ID },
  )
  return buildCdpSubprojectIndex(subprojects)
}
