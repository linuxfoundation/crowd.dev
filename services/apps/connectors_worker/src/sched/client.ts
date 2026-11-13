const CALL_INTERVAL_MS = 5_000
const CALL_TIMEOUT_MS = 120_000
const USER_AGENT = 'crowd.dev-sched-import/1.0 (+https://www.linuxfoundation.org)'
const ERROR_BODY_PREFIX = 'ERR:'

export class SchedRateLimitError extends Error {
  constructor(subdomain: string, path: string) {
    super(`${subdomain}: rate limited on ${path}`)
  }
}

export class SchedCallError extends Error {
  constructor(
    message: string,
    readonly authFailed = false,
  ) {
    super(message)
  }
}

let lastCallAt = 0

async function waitForTurn(): Promise<void> {
  const wait = lastCallAt + CALL_INTERVAL_MS - Date.now()
  if (wait > 0) {
    await new Promise((resolve) => setTimeout(resolve, wait))
  }
  lastCallAt = Date.now()
}

// The URL carries the key and some responses echo it, so neither is ever logged or put in an error.
export async function callSched(
  subdomain: string,
  key: string,
  path: string,
  params: Record<string, string> = {},
): Promise<unknown> {
  await waitForTurn()

  const url = new URL(`https://${subdomain}.sched.com/api/${path}`)
  url.searchParams.set('api_key', key)
  url.searchParams.set('format', 'json')
  for (const [name, value] of Object.entries(params)) {
    url.searchParams.set(name, value)
  }

  const response = await fetch(url, {
    headers: { Accept: 'application/json', 'User-Agent': USER_AGENT },
    signal: AbortSignal.timeout(CALL_TIMEOUT_MS),
  })

  if (response.status === 429) {
    throw new SchedRateLimitError(subdomain, path)
  }

  const body = await response.text()
  const trimmed = body.trimStart()
  if (trimmed.startsWith(ERROR_BODY_PREFIX)) {
    const authFailed = /authoriz/i.test(trimmed.slice(0, 100))
    throw new SchedCallError(
      `${subdomain}: ${path} returned ${authFailed ? 'an authorization error' : 'an error'}`,
      authFailed,
    )
  }
  if (!response.ok) {
    throw new SchedCallError(`${subdomain}: ${path} returned HTTP ${response.status}`)
  }

  try {
    return JSON.parse(body)
  } catch {
    throw new SchedCallError(`${subdomain}: ${path} did not return JSON`)
  }
}
