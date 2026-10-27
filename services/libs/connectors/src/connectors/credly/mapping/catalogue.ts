import { normalizeSku, skuBase } from './courseCode'

export interface CatalogueCourse {
  sku: string
  title: string
  courseGroupId: string | null
  isTestOrArchived: boolean
}

export interface CatalogueCourseGroup {
  courseGroupId: string
  title: string
  projectSlug: string
}

export interface CatalogueEnrollment {
  courseCode: string
  projectSlug: string
  enrollments: number
}

export interface Catalogue {
  courses: CatalogueCourse[]
  courseGroups: CatalogueCourseGroup[]
  enrollments: CatalogueEnrollment[]
}

export interface TitleMatch {
  projectSlug: string
  source: string
}

export interface CatalogueIndex {
  coursesBySku: Map<string, CatalogueCourse[]>
  coursesByBase: Map<string, CatalogueCourse[]>
  projectByGroup: Map<string, string>
  enrollmentsByCode: Map<string, CatalogueEnrollment[]>
  projectsByTitle: Map<string, TitleMatch[]>
}

// Catalogue titles carry the code and the archive flag in parentheses: "Intro (LFS158) (Archived)".
const TITLE_NOISE = /\s*\((?:[A-Z][A-Z0-9-]{1,14}|Archived|ARCHIVED)\)/g

export function normalizeTitle(title: string): string {
  return title.replace(TITLE_NOISE, '').replace(/\s+/g, ' ').trim().toLowerCase()
}

function push<T>(map: Map<string, T[]>, key: string, value: T): void {
  const list = map.get(key)
  if (list) list.push(value)
  else map.set(key, [value])
}

export function indexCatalogue(catalogue: Catalogue): CatalogueIndex {
  const index: CatalogueIndex = {
    coursesBySku: new Map(),
    coursesByBase: new Map(),
    projectByGroup: new Map(),
    enrollmentsByCode: new Map(),
    projectsByTitle: new Map(),
  }
  for (const group of catalogue.courseGroups) {
    index.projectByGroup.set(group.courseGroupId, group.projectSlug)
    push(index.projectsByTitle, normalizeTitle(group.title), {
      projectSlug: group.projectSlug,
      source: group.title,
    })
  }
  for (const course of catalogue.courses) {
    if (!course.sku.trim()) continue
    push(index.coursesBySku, normalizeSku(course.sku), course)
    push(index.coursesByBase, skuBase(course.sku), course)
    const projectSlug = course.courseGroupId
      ? index.projectByGroup.get(course.courseGroupId)
      : undefined
    if (projectSlug) {
      push(index.projectsByTitle, normalizeTitle(course.title), {
        projectSlug,
        source: course.sku,
      })
    }
  }
  for (const enrollment of catalogue.enrollments) {
    push(index.enrollmentsByCode, normalizeSku(enrollment.courseCode), enrollment)
  }
  return index
}
