// A registry fetch outcome that should be given up on rather than retried: 404, or any other
// 4xx client error (e.g. a malformed/illegal project name that leaked into `packages`). 429 is
// excluded — it's transient and rides the slow (Temporal-retry) path. Shared by the npm and pypi
// workers, whose fetchers classify HTTP outcomes into these (statusCode, kind) pairs.
export function isClientError(code: number | undefined, kind: string): boolean {
  return kind === 'NOT_FOUND' || (code !== undefined && code >= 400 && code < 500 && code !== 429)
}
