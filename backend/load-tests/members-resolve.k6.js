import http from 'k6/http'
import { check, sleep } from 'k6'
import { Counter, Rate } from 'k6/metrics'

// Custom metrics
const rateLimited = new Counter('rate_limited_429')
const errorRate = new Rate('errors')

const PROFILE = __ENV.PROFILE || 'steady'
const API_BASE_URL = __ENV.API_BASE_URL || 'https://api.staging.crowd.dev'
const ALLOW_PROD = __ENV.ALLOW_PROD === '1'

const PROD_PATTERN = /prod(uction)?\.crowd\.dev|lfx\.linuxfoundation\.org/i

// Fail-safe guard: require explicit opt-in for prod URLs
if (PROD_PATTERN.test(API_BASE_URL) && !ALLOW_PROD) {
  throw new Error(
    `Production URL detected. Set ALLOW_PROD=1 to confirm you have coordinated a maintenance window.`,
  )
}

const PROFILES = {
  steady: {
    scenarios: {
      steady: {
        executor: 'constant-arrival-rate',
        rate: 200,
        timeUnit: '1m',
        duration: '10m',
        preAllocatedVUs: 20,
        maxVUs: 50,
      },
    },
  },
  burst: {
    scenarios: {
      burst: {
        executor: 'ramping-arrival-rate',
        startRate: 0,
        timeUnit: '1m',
        stages: [
          { target: 400, duration: '2m' },
          { target: 400, duration: '5m' },
          { target: 0, duration: '1m' },
        ],
        preAllocatedVUs: 40,
        maxVUs: 100,
      },
    },
  },
  soak: {
    scenarios: {
      soak: {
        executor: 'constant-arrival-rate',
        rate: 200,
        timeUnit: '1m',
        duration: '60m',
        preAllocatedVUs: 20,
        maxVUs: 50,
      },
    },
  },
}

export const options = {
  ...PROFILES[PROFILE],
  thresholds: {
    'http_req_duration{status:200}': ['p(95)<500', 'p(99)<2000'],
    errors: ['rate<0.01'],
    // During 'burst', 429s are expected — this counter is informational only
    rate_limited_429: [],
  },
}

// Fetch M2M token once before the test run
export function setup() {
  const tokenRes = http.post(
    __ENV.AUTH0_TOKEN_URL,
    JSON.stringify({
      grant_type: 'client_credentials',
      client_id: __ENV.AUTH0_CLIENT_ID,
      client_secret: __ENV.AUTH0_CLIENT_SECRET,
      audience: __ENV.AUTH0_AUDIENCE,
    }),
    { headers: { 'Content-Type': 'application/json' } },
  )

  if (tokenRes.status !== 200) {
    throw new Error(`Failed to fetch M2M token: ${tokenRes.status} ${tokenRes.body}`)
  }

  return { token: tokenRes.json('access_token') }
}

// Fixture LFIDs — override by setting LFIDS_FILE to a JSON array path
const LFIDS = __ENV.LFIDS
  ? JSON.parse(__ENV.LFIDS)
  : ['testuser1', 'testuser2', 'testuser3', 'testuser4', 'testuser5']

let cursor = 0

export default function (data) {
  const lfid = LFIDS[cursor % LFIDS.length]
  cursor++

  const res = http.post(
    `${API_BASE_URL}/api/public/v1/members/resolve`,
    JSON.stringify({ lfids: [lfid] }),
    {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${data.token}`,
      },
      tags: { endpoint: 'resolve' },
    },
  )

  const ok =
    res.status === 200 ||
    res.status === 404 ||
    res.status === 409 ||
    res.status === 429

  check(res, { 'status is expected': () => ok })

  if (res.status === 429) {
    rateLimited.add(1)
  } else if (!ok) {
    errorRate.add(1)
  } else {
    errorRate.add(0)
  }

  // Honour Retry-After if rate limited; otherwise no sleep (arrival-rate executor paces requests)
  if (res.status === 429) {
    const retryAfter = parseInt(res.headers['Retry-After'] || '60', 10)
    sleep(retryAfter)
  }
}
