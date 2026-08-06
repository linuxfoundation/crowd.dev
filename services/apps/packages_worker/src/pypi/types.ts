export type FetchErrorKind = 'RATE_LIMIT' | 'TRANSIENT' | 'NOT_FOUND' | 'MALFORMED'

export interface FetchError {
  kind: FetchErrorKind
  message: string
  statusCode?: number
}

export function isFetchError(v: unknown): v is FetchError {
  return typeof v === 'object' && v !== null && 'kind' in v && 'message' in v
}

// Subset of the PyPI JSON API project response we consume.
// https://pypi.org/pypi/<name>/json — see docs.pypi.org/api/json.
export interface PyPiReleaseFile {
  upload_time_iso_8601?: string
  yanked?: boolean
}

export interface PyPiInfo {
  name: string
  version?: string
  summary?: string | null
  author?: string | null
  author_email?: string | null
  maintainer?: string | null
  maintainer_email?: string | null
  license?: string | null
  // PEP 639 SPDX license expression (newer packages).
  license_expression?: string | null
  keywords?: string | null
  classifiers?: string[]
  home_page?: string | null
  // Canonical PyPI project URL, e.g. https://pypi.org/project/<name>/
  package_url?: string | null
  project_urls?: Record<string, string> | null
  // True when the latest release is yanked.
  yanked?: boolean
}

export interface PyPiProject {
  info: PyPiInfo
  // version string -> array of distribution file objects (may be empty).
  releases?: Record<string, PyPiReleaseFile[]>
}
