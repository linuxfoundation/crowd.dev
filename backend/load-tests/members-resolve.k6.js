import http from 'k6/http'
import { check } from 'k6'
import { Counter, Rate } from 'k6/metrics'

const rateLimited = new Counter('rate_limited_429')
const errorRate = new Rate('errors')

const PROFILE = __ENV.PROFILE || 'steady'
const API_BASE_URL = __ENV.API_BASE_URL || 'https://api.staging.crowd.dev'
const ALLOW_PROD = __ENV.ALLOW_PROD === '1'

const PROD_HOSTS = /^(api\.crowd\.dev|cm\.lfx\.linuxfoundation\.org)(:\d+)?$/

const parsedHost = new URL(API_BASE_URL).host

if (PROD_HOSTS.test(parsedHost) && !ALLOW_PROD) {
  throw new Error(
    `Production host detected (${parsedHost}). Set ALLOW_PROD=1 to confirm you have coordinated a maintenance window.`,
  )
}

const VALID_PROFILES = ['steady', 'burst', 'soak']

if (!VALID_PROFILES.includes(PROFILE)) {
  throw new Error(`Unknown PROFILE="${PROFILE}". Valid values: ${VALID_PROFILES.join(', ')}`)
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
    thresholds: {
      'http_req_duration{status:200}': ['p(95)<500', 'p(99)<2000'],
      errors: ['rate<0.01'],
      rate_limited_429: ['count==0'],
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
    thresholds: {
      errors: ['rate<0.01'],
      rate_limited_429: ['count>0'],
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
    thresholds: {
      'http_req_duration{status:200}': ['p(95)<500', 'p(99)<2000'],
      errors: ['rate<0.01'],
      rate_limited_429: ['count==0'],
    },
  },
}

export const options = PROFILES[PROFILE]

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

const LFIDS = __ENV.LFIDS
  ? JSON.parse(__ENV.LFIDS)
  : ['testuser1', 'testuser2', 'testuser3', 'testuser4', 'testuser5']

let cursor = 0

export default function main(data) {
  const lfid = LFIDS[cursor % LFIDS.length]
  cursor += 1

  const res = http.post(
    `${API_BASE_URL}/api/public/v1/members/resolve`,
    JSON.stringify({ lfids: [lfid] }),
    {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${data.token}`,
      },
    },
  )

  const valid = res.status === 200 || res.status === 404 || res.status === 409 || res.status === 429

  check(res, { 'status is expected': () => valid })

  if (res.status === 429) {
    rateLimited.add(1)
    errorRate.add(0)
  } else if (!valid) {
    errorRate.add(1)
  } else {
    errorRate.add(0)
  }
}
