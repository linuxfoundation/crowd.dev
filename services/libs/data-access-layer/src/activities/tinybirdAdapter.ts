import { getServiceLogger } from '@crowd/logging'

import { IQueryActivitiesParameters } from './types'

const log = getServiceLogger()

/* =========================
 * Constants & basic helpers
 * ========================= */

const DEFAULT_PAGE_SIZE = 20 as const

type OrderValue = 'timestamp_DESC' | 'timestamp_ASC' | 'createdAt_DESC' | 'createdAt_ASC'
const ORDER_ALLOW: ReadonlySet<OrderValue> = new Set([
  'timestamp_DESC',
  'timestamp_ASC',
  'createdAt_DESC',
  'createdAt_ASC',
])

/** Pick the first valid orderBy, default to 'timestamp_DESC' */
const pickOrder = (orderBy?: string[]): OrderValue => {
  if (!orderBy || orderBy.length === 0) return 'timestamp_DESC'
  const first = (
    orderBy.find((o) => typeof o === 'string' && o.trim().length > 0) || ''
  ).trim() as OrderValue
  return ORDER_ALLOW.has(first) ? first : 'timestamp_DESC'
}

/** Type guard for non-empty strings */
const isNonEmptyString = (v: unknown): v is string => typeof v === 'string' && v.trim().length > 0

/** Normalize unknown into a trimmed, unique string[] (or undefined). */
const toStringArray = (x: unknown): string[] | undefined => {
  if (typeof x === 'string') {
    const t = x.trim()
    return t ? [t] : undefined
  }
  if (Array.isArray(x)) {
    const out = Array.from(new Set(x.map((s) => String(s).trim()).filter(Boolean)))
    return out.length ? out : undefined
  }
  return undefined
}

/** Public Tinybird params shape (JSON-serializable) */
export type TBParams = Record<string, string | number | boolean | string[] | undefined>

/* =========================
 * Date normalization
 * ========================= */

/**
 * Best-effort conversion of any input into a clean UTC ISO string.
 * - Accepts Date instances, epoch numbers (ms or s), and strings (ISO-like, GMT with parentheses, etc.)
 * - Cleans oddities like extra spaces in time components (e.g., "15: 01: 25")
 * - Returns undefined when parsing fails
 */
export const normalizeDate = (input: unknown): string | undefined => {
  if (input == null) return undefined

  // Date object
  if (input instanceof Date && !isNaN(input.getTime())) return input.toISOString()

  // Numeric epoch: if small (likely seconds), convert to ms
  if (typeof input === 'number') {
    const n = input > 1e12 ? input : input > 1e10 ? input : input * 1000
    const d = new Date(n)
    return isNaN(d.getTime()) ? undefined : d.toISOString()
  }

  // Strings: sanitize and try multiple parse strategies
  if (typeof input === 'string') {
    let s = input.trim()

    // Keep only a reasonable prefix (defensive against log tails)
    if (s.length > 200) s = s.slice(0, 200)

    // Collapse spaces after colons in the time portion: "15: 01: 25" -> "15:01:25"
    s = s.replace(/:\s+/g, ':')

    // Drop trailing parenthetical TZ names: " (Coordinated Universal Time)"
    s = s.replace(/\s*\([^)]+\)\s*$/, '')

    // Try direct parse
    let d = new Date(s)
    if (!isNaN(d.getTime())) return d.toISOString()

    // Fallback: extract ISO-like fragment
    const isoLike = s.match(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?/i)
    if (isoLike) {
      d = new Date(isoLike[0].endsWith('Z') ? isoLike[0] : isoLike[0] + 'Z')
      if (!isNaN(d.getTime())) return d.toISOString()
    }

    // Fallback: RFC-like with GMT offset (e.g., "Tue Sep 16 2025 15:40:08 GMT+0000")
    const gmtLike = s.match(
      /(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+GMT[+-]\d{4}/,
    )

    if (gmtLike) {
      d = new Date(gmtLike[0])
      if (!isNaN(d.getTime())) return d.toISOString()
    }

    return undefined
  }

  return undefined
}

/* =========================
 * Groups typing & utilities
 * ========================= */

type GroupFilter = Partial<{
  memberIds: string[] | string
  memberIds_exclude: string[] | string
  activityTypes: string[] | string
  activityTypes_exclude: string[] | string
  organizationIds: string[] | string
  organizationIds_exclude: string[] | string
  platforms: string[] | string
  platforms_exclude: string[] | string
  channels: string[] | string
  channels_exclude: string[] | string
  ids: string[] | string
  ids_exclude: string[] | string
}>

type GroupKey = keyof GroupFilter
const INCLUDE_KEYS = [
  'memberIds',
  'activityTypes',
  'organizationIds',
  'platforms',
  'channels',
  'ids',
] as const
const EXCLUDE_KEYS = [
  'memberIds_exclude',
  'activityTypes_exclude',
  'organizationIds_exclude',
  'platforms_exclude',
  'channels_exclude',
  'ids_exclude',
] as const
const ALL_KEYS: readonly GroupKey[] = [...INCLUDE_KEYS, ...EXCLUDE_KEYS]

/** Reuse array normalization for group values */
const normArr = (x?: string | string[]): string[] | undefined => toStringArray(x)

const getVals = <K extends GroupKey>(g: GroupFilter, k: K): string[] | undefined => normArr(g[k])
const setVals = <K extends GroupKey>(g: GroupFilter, k: K, v?: string[] | string): void => {
  if (v !== undefined) g[k] = v
  else delete g[k]
}

/** Merge b into a (union of values per key) */
const mergeGroup = (a: GroupFilter, b: GroupFilter): GroupFilter => {
  const out: GroupFilter = { ...a }
  for (const k of ALL_KEYS) {
    const na = getVals(a, k)
    const nb = getVals(b, k)
    if (na && nb) setVals(out, k, Array.from(new Set([...na, ...nb])))
    else if (nb) setVals(out, k, nb)
  }
  return out
}

/* =========================
 * Public args (extended)
 * ========================= */

type ExtendedArgs = IQueryActivitiesParameters & {
  /** Segment ids narrowed to string[] | string for this adapter */
  segmentIds?: string[] | string

  /** Optional explicit search term override */
  searchTerm?: string
  /** Optional pre-built OR groups */
  groups?: GroupFilter[]
  /** Logical filter (and/or/not, leaves) */
  filter?: unknown
  /** Count mode (return only count) */
  countOnly?: boolean

  /** Pipe base filters (pass-throughs) */
  platform?: string
  activity_type?: string
  activity_types?: string[] | string
  repos?: string[] | string
  onlyContributions?: boolean
  indirectFork?: number | boolean

  /** Pagination & order — usually present in IQueryActivitiesParameters */
  limit?: number
  offset?: number
  noLimit?: boolean
  orderBy?: string[]
}

/* =========================
 * Logical filter parsing
 * ========================= */

type LeafField =
  | 'platform'
  | 'channel'
  | 'type'
  | 'organizationId'
  | 'memberId'
  | 'id'
  | 'title'
  | 'body'
  | 'timestamp'

/** Map a leaf field to include/exclude group keys */
const KEY_MAP = {
  platform: { inc: 'platforms', exc: 'platforms_exclude' },
  channel: { inc: 'channels', exc: 'channels_exclude' },
  type: { inc: 'activityTypes', exc: 'activityTypes_exclude' },
  organizationId: { inc: 'organizationIds', exc: 'organizationIds_exclude' },
  memberId: { inc: 'memberIds', exc: 'memberIds_exclude' },
  id: { inc: 'ids', exc: 'ids_exclude' },
} as const satisfies Record<
  Exclude<LeafField, 'timestamp' | 'title' | 'body'>,
  { inc: GroupKey; exc: GroupKey }
>

/* ---- Delta types: discriminated union ---- */

type MetaDelta = { startDate?: string; endDate?: string; searchTerm?: string; ignored?: true }
type Delta = { kind: 'group'; group: GroupFilter } | { kind: 'meta'; meta: MetaDelta }

const asGroup = (g: GroupFilter): Delta => ({ kind: 'group', group: g })
const asMeta = (m: MetaDelta): Delta => ({ kind: 'meta', meta: m })

/**
 * Convert a single leaf predicate into:
 * - a group delta (include/exclude lists), or
 * - a meta delta (startDate/endDate/searchTerm), or
 * - ignored (e.g., timestamp inside OR/negation)
 */
const leafToDelta = (field: LeafField, pred: unknown, neg: boolean, inOr: boolean): Delta => {
  // timestamp → collect base AND window only (ignore inside OR/negation)
  if (field === 'timestamp') {
    if (inOr) return asMeta({ ignored: true })
    if (neg) return asMeta({ ignored: true })
    const p = pred as { gte?: unknown; gt?: unknown; lte?: unknown; lt?: unknown }
    const meta: MetaDelta = {}
    const sd = normalizeDate(p?.gte ?? p?.gt)
    const ed = normalizeDate(p?.lte ?? p?.lt)
    if (sd) meta.startDate = sd
    if (ed) meta.endDate = ed
    return asMeta(meta)
  }

  // textContains → global searchTerm (AND with everything); negated is ignored
  if (
    typeof pred === 'object' &&
    pred !== null &&
    'textContains' in (pred as Record<string, unknown>)
  ) {
    const raw = (pred as Record<'textContains', unknown>).textContains
    const val = typeof raw === 'string' ? raw.trim() : String(raw ?? '').trim()
    if (!val) return asMeta({})
    if (neg) return asMeta({ ignored: true })
    return asMeta({ searchTerm: val })
  }

  // include/exclude → groups
  if (field in KEY_MAP) {
    const { inc, exc } = KEY_MAP[field as keyof typeof KEY_MAP]
    const pv = pred as { in?: string[] | string; eq?: string }
    const values = toStringArray(pv?.in) ?? toStringArray(pv?.eq)
    if (!values || !values.length) return asMeta({})
    const g: GroupFilter = neg
      ? ({ [exc]: values } as GroupFilter)
      : ({ [inc]: values } as GroupFilter)
    return asGroup(g)
  }

  return asMeta({})
}

/** If all OR branches are include-only on the same key → compress into a single merged group */
const getSingleIncludeKey = (g: GroupFilter): GroupKey | null => {
  for (const k of EXCLUDE_KEYS) if (getVals(g, k)) return null
  const present: GroupKey[] = []
  for (const k of INCLUDE_KEYS) if (getVals(g, k)) present.push(k)
  return present.length === 1 ? present[0] : null
}

const compressHomogeneousOrBranches = (branches: GroupFilter[]): GroupFilter[] => {
  const nonEmpty = branches.filter((g) => Object.keys(g).length > 0)
  if (nonEmpty.length <= 1) return nonEmpty
  const firstKey = getSingleIncludeKey(nonEmpty[0])
  if (!firstKey) return nonEmpty
  for (let i = 1; i < nonEmpty.length; i++) {
    if (getSingleIncludeKey(nonEmpty[i]) !== firstKey) return nonEmpty
  }
  const merged: GroupFilter = {}
  const allVals = nonEmpty.flatMap((g) => getVals(g, firstKey) ?? [])
  setVals(merged, firstKey, Array.from(new Set(allVals)))
  return [merged]
}

/** Runtime guards for unknown input */
const isObj = (x: unknown): x is Record<string, unknown> => typeof x === 'object' && x !== null
const isArr = (x: unknown): x is unknown[] => Array.isArray(x)

type NodeCtx = { neg: boolean; inOr: boolean; intoGroup?: GroupFilter }
type Parsed = { groups: GroupFilter[]; startDate?: string; endDate?: string; searchTerm?: string }

/**
 * Parse the logical filter (and/or/not + leaves) into:
 * - groups: array of OR-able include/exclude group filters,
 * - startDate, endDate: base time window (AND),
 * - searchTerm: global text search term (AND).
 */
function parseLogicalFilter(filter: unknown): Parsed {
  if (!isObj(filter)) return { groups: [] }

  const skeleton: GroupFilter = {}
  let startDate: string | undefined
  let endDate: string | undefined
  let searchTerm: string | undefined
  const orGroups: GroupFilter[] = []

  const absorbSearch = (val?: string) => {
    if (!val) return
    if (!searchTerm) searchTerm = val
    else if (val !== searchTerm)
      console.warn(
        `[buildActivitiesParams] Multiple textContains ("${searchTerm}" vs "${val}"). Using the first.`,
      )
  }

  const handleNode = (node: unknown, ctx: NodeCtx): void => {
    if (!isObj(node)) return

    // NOT
    if ('not' in node) {
      handleNode((node as Record<string, unknown>).not, {
        neg: !ctx.neg,
        inOr: ctx.inOr,
        intoGroup: ctx.intoGroup,
      })
    }

    let compositeHandled = false

    // AND
    if ('and' in node && isArr((node as Record<string, unknown>).and)) {
      for (const sub of (node as Record<string, unknown>).and as unknown[]) {
        handleNode(sub, { neg: ctx.neg, inOr: ctx.inOr, intoGroup: ctx.intoGroup })
      }
      compositeHandled = true
    }

    // OR
    if ('or' in node && isArr((node as Record<string, unknown>).or)) {
      const branches: GroupFilter[] = []
      for (const opt of (node as Record<string, unknown>).or as unknown[]) {
        const gLocal: GroupFilter = {}
        const prevSearch = searchTerm
        handleNode(opt, { neg: ctx.neg, inOr: true, intoGroup: gLocal })
        if (Object.keys(gLocal).length > 0) branches.push(gLocal)
        if (prevSearch && searchTerm && searchTerm !== prevSearch) {
          /* already warned */
        }
      }
      const compressed = compressHomogeneousOrBranches(branches)
      compressed.forEach((g) => orGroups.push(g))
      compositeHandled = true
    }

    if (compositeHandled) return

    // Leaves
    const fields: LeafField[] = [
      'timestamp',
      'platform',
      'channel',
      'type',
      'organizationId',
      'memberId',
      'id',
      'title',
      'body',
    ]
    for (const f of fields) {
      if (f in node) {
        const delta = leafToDelta(f, (node as Record<string, unknown>)[f], ctx.neg, ctx.inOr)
        if (delta.kind === 'meta') {
          const { startDate: sd, endDate: ed, searchTerm: st } = delta.meta
          if (sd) startDate = sd
          if (ed) endDate = ed
          if (st) absorbSearch(st)
        } else {
          const target = ctx.intoGroup ?? skeleton
          Object.assign(target, mergeGroup(target, delta.group))
        }
      }
    }
  }

  // Root: accumulate base AND leaves into skeleton
  handleNode(filter, { neg: false, inOr: false, intoGroup: undefined })

  // Build final groups
  const groups =
    orGroups.length > 0
      ? orGroups.map((g) => mergeGroup(skeleton, g))
      : Object.keys(skeleton).length
        ? [skeleton]
        : []

  return { groups, startDate, endDate, searchTerm }
}

/* =========================
 * Emit Tinybird params
 * ========================= */

const emitGroup = (params: TBParams, n: number, g: GroupFilter): void => {
  const pref = `G${n}_`
  const set = (k: keyof GroupFilter, name: string) => {
    const v = getVals(g, k)
    if (v && v.length) params[`${pref}${name}`] = v
  }

  set('memberIds', 'memberIds')
  set('memberIds_exclude', 'memberIds_exclude')
  set('activityTypes', 'activityTypes')
  set('activityTypes_exclude', 'activityTypes_exclude')
  set('organizationIds', 'organizationIds')
  set('organizationIds_exclude', 'organizationIds_exclude')
  set('platforms', 'platforms')
  set('platforms_exclude', 'platforms_exclude')
  set('channels', 'channels')
  set('channels_exclude', 'channels_exclude')
  set('ids', 'ids')
  set('ids_exclude', 'ids_exclude')
}

/* =========================
 * Public builder
 * ========================= */

/**
 * Build the Tinybird parameter map from ExtendedArgs:
 * - Outputs arrays (not CSV) for all Array(T) params used by the pipe.
 * - Parses a logical filter into OR-able groups + meta (searchTerm / start/end dates).
 * - Normalizes startDate/endDate into clean UTC ISO strings.
 * - Paginates via page/pageSize and sets orderBy.
 */
export function buildActivitiesParams(arg: ExtendedArgs): TBParams {
  const params: TBParams = {}

  // segments as Array(String) — cap at 500 to avoid Tinybird 400 on large project groups.
  // When an entity filter (memberId/orgId) is present this slice is a preview; users can
  // drill into a specific segment for full results.
  const MAX_SEGMENTS = 600
  const allSegments = toStringArray(arg.segmentIds) ?? []
  if (allSegments.length > MAX_SEGMENTS) {
    log.warn(
      { totalSegments: allSegments.length, cappedAt: MAX_SEGMENTS },
      'Tinybird segment list truncated — pass a specific segmentId to see full results',
    )
  }
  const segments = allSegments.slice(0, MAX_SEGMENTS)
  if (segments.length) params.segments = segments

  // Optional pass-throughs (arrays, strings, booleans)
  const repos = toStringArray(arg.repos)
  if (repos && repos.length) params.repos = repos

  const activity_types = toStringArray(arg.activity_types)
  if (activity_types && activity_types.length) params.activity_types = activity_types

  if (isNonEmptyString(arg.platform)) {
    params.platform = arg.platform.trim()
  }
  if (isNonEmptyString(arg.activity_type)) {
    params.activity_type = arg.activity_type.trim()
  }
  if (typeof arg.onlyContributions === 'boolean') {
    params.onlyContributions = arg.onlyContributions ? 1 : 0
  }
  if (typeof arg.indirectFork === 'boolean') {
    params.indirectFork = arg.indirectFork ? 1 : 0
  }

  // pagination
  const pageSize = arg.noLimit === true ? 0 : (arg.limit ?? DEFAULT_PAGE_SIZE)
  const offset = arg.offset ?? 0
  params.pageSize = pageSize
  params.page = pageSize > 0 ? Math.floor(offset / Math.max(1, pageSize)) : 0

  // order & count
  params.orderBy = pickOrder(arg.orderBy)
  if (arg.countOnly) params.countOnly = 1

  // parse logical filter (preferred)
  const parsed = parseLogicalFilter(arg.filter)

  // search term (explicit override wins)
  const st = isNonEmptyString(arg.searchTerm) ? arg.searchTerm.trim() : parsed.searchTerm
  if (st) params.searchTerm = st

  // base time window (AND) — normalize/clean just before emitting
  const normalizedStart = normalizeDate(parsed.startDate)
  const normalizedEnd = normalizeDate(parsed.endDate)
  if (normalizedStart) params.startDate = normalizedStart
  if (normalizedEnd) params.endDate = normalizedEnd

  // If both present and out of order, swap to ensure start <= end
  if (
    typeof params.startDate === 'string' &&
    typeof params.endDate === 'string' &&
    params.startDate > params.endDate
  ) {
    const tmp = params.startDate
    params.startDate = params.endDate
    params.endDate = tmp
  }

  // groups (max 5)
  const MAX = 5
  if (parsed.groups.length > MAX) {
    console.warn(
      `[buildActivitiesParams] Received ${parsed.groups.length} OR groups; only ${MAX} supported. Extra groups will be ignored.`,
    )
  }
  parsed.groups.slice(0, MAX).forEach((g, i) => emitGroup(params, i + 1, g))

  return params
}
