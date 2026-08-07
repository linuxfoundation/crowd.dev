import { afterEach, describe, expect, it } from 'vitest'

import { resolveAgentAuth } from '@crowd/common'

const AKRITES_BEDROCK_ENV_VAR_NAMES = {
  accessKeyId: 'AKRITES_AWS_BEDROCK_ACCESS_KEY_ID',
  secretAccessKey: 'AKRITES_AWS_BEDROCK_SECRET_ACCESS_KEY',
  region: 'AKRITES_AWS_BEDROCK_REGION',
}

const ALL_ENV_VARS = [
  'CROWD_AWS_BEDROCK_ACCESS_KEY_ID',
  'CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY',
  'CROWD_AWS_BEDROCK_REGION',
  'AKRITES_AWS_BEDROCK_ACCESS_KEY_ID',
  'AKRITES_AWS_BEDROCK_SECRET_ACCESS_KEY',
  'AKRITES_AWS_BEDROCK_REGION',
  'BLAST_RADIUS_ANTHROPIC_API_KEY',
  'BLAST_RADIUS_ANTHROPIC_BASE_URL',
  'ANTHROPIC_API_KEY',
]

function clearAuthEnv() {
  for (const key of ALL_ENV_VARS) {
    delete process.env[key]
  }
}

describe('resolveAgentAuth', () => {
  afterEach(() => {
    clearAuthEnv()
  })

  it('resolves bedrock mode using the default (CROWD_AWS_BEDROCK_*) env vars', () => {
    clearAuthEnv()
    process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID = 'AKIA_TEST'
    process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY = 'secret'
    process.env.ANTHROPIC_API_KEY = 'sk-ant-should-not-survive'

    const auth = resolveAgentAuth()

    expect(auth.mode).toBe('bedrock')
    expect(auth.env.CLAUDE_CODE_USE_BEDROCK).toBe('1')
    expect(auth.env.AWS_ACCESS_KEY_ID).toBe('AKIA_TEST')
    expect(auth.env.AWS_SECRET_ACCESS_KEY).toBe('secret')
    expect(auth.env.AWS_REGION).toBe('us-east-1')
    expect(auth.env.ANTHROPIC_API_KEY).toBeUndefined()
  })

  it('resolves bedrock mode using a caller-supplied bedrockEnvVarNames (e.g. Akrites)', () => {
    clearAuthEnv()
    process.env.AKRITES_AWS_BEDROCK_ACCESS_KEY_ID = 'AKIA_AKRITES'
    process.env.AKRITES_AWS_BEDROCK_SECRET_ACCESS_KEY = 'akrites-secret'
    // A CROWD_AWS_BEDROCK_* credential being set for an unrelated consumer must not
    // leak into an Akrites-scoped caller.
    process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID = 'AKIA_UNRELATED'
    process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY = 'unrelated-secret'

    const auth = resolveAgentAuth({ bedrockEnvVarNames: AKRITES_BEDROCK_ENV_VAR_NAMES })

    expect(auth.mode).toBe('bedrock')
    expect(auth.env.AWS_ACCESS_KEY_ID).toBe('AKIA_AKRITES')
    expect(auth.env.AWS_SECRET_ACCESS_KEY).toBe('akrites-secret')
  })

  it('uses the region env var named by bedrockEnvVarNames when set', () => {
    clearAuthEnv()
    process.env.AKRITES_AWS_BEDROCK_ACCESS_KEY_ID = 'AKIA_AKRITES'
    process.env.AKRITES_AWS_BEDROCK_SECRET_ACCESS_KEY = 'akrites-secret'
    process.env.AKRITES_AWS_BEDROCK_REGION = 'us-west-2'

    const auth = resolveAgentAuth({ bedrockEnvVarNames: AKRITES_BEDROCK_ENV_VAR_NAMES })

    expect(auth.env.AWS_REGION).toBe('us-west-2')
  })

  it('does not fall into bedrock mode with only one of the two credentials', () => {
    clearAuthEnv()
    process.env.AKRITES_AWS_BEDROCK_ACCESS_KEY_ID = 'AKIA_AKRITES'
    process.env.BLAST_RADIUS_ANTHROPIC_API_KEY = 'sk-ant-fallback'

    const auth = resolveAgentAuth({ bedrockEnvVarNames: AKRITES_BEDROCK_ENV_VAR_NAMES })

    expect(auth.mode).toBe('anthropic-api-key')
  })

  it('resolves anthropic-api-key mode when no bedrock credentials are set', () => {
    clearAuthEnv()
    process.env.BLAST_RADIUS_ANTHROPIC_API_KEY = 'sk-ant-test'
    process.env.BLAST_RADIUS_ANTHROPIC_BASE_URL = 'https://litellm.internal'

    const auth = resolveAgentAuth({ bedrockEnvVarNames: AKRITES_BEDROCK_ENV_VAR_NAMES })

    expect(auth.mode).toBe('anthropic-api-key')
    expect(auth.env.ANTHROPIC_API_KEY).toBe('sk-ant-test')
    expect(auth.env.ANTHROPIC_BASE_URL).toBe('https://litellm.internal')
    expect(auth.resolveModel('claude-opus-4-8')).toBe('claude-opus-4-8')
  })

  it('supports a custom api key env var name', () => {
    clearAuthEnv()
    process.env.CUSTOM_ANTHROPIC_API_KEY = 'sk-ant-custom'

    const auth = resolveAgentAuth({ apiKeyEnvVar: 'CUSTOM_ANTHROPIC_API_KEY' })

    expect(auth.mode).toBe('anthropic-api-key')
    expect(auth.env.ANTHROPIC_API_KEY).toBe('sk-ant-custom')

    delete process.env.CUSTOM_ANTHROPIC_API_KEY
  })

  it('falls back to cli auth when nothing is configured', () => {
    clearAuthEnv()

    const auth = resolveAgentAuth({ bedrockEnvVarNames: AKRITES_BEDROCK_ENV_VAR_NAMES })

    expect(auth.mode).toBe('cli-fallback')
    expect(auth.env).toBeUndefined()
    expect(auth.resolveModel('claude-sonnet-5')).toBe('claude-sonnet-5')
  })

  it('translates model IDs via modelBedrockMap in bedrock mode', () => {
    clearAuthEnv()
    process.env.AKRITES_AWS_BEDROCK_ACCESS_KEY_ID = 'AKIA_AKRITES'
    process.env.AKRITES_AWS_BEDROCK_SECRET_ACCESS_KEY = 'akrites-secret'

    const auth = resolveAgentAuth({
      bedrockEnvVarNames: AKRITES_BEDROCK_ENV_VAR_NAMES,
      modelBedrockMap: { 'claude-opus-4-8': 'us.anthropic.claude-opus-4-8-v1:0' },
    })

    expect(auth.resolveModel('claude-opus-4-8')).toBe('us.anthropic.claude-opus-4-8-v1:0')
  })

  it('throws on an unmapped model when a modelBedrockMap is provided', () => {
    clearAuthEnv()
    process.env.AKRITES_AWS_BEDROCK_ACCESS_KEY_ID = 'AKIA_AKRITES'
    process.env.AKRITES_AWS_BEDROCK_SECRET_ACCESS_KEY = 'akrites-secret'

    const auth = resolveAgentAuth({
      bedrockEnvVarNames: AKRITES_BEDROCK_ENV_VAR_NAMES,
      modelBedrockMap: { 'claude-opus-4-8': 'us.anthropic.claude-opus-4-8-v1:0' },
    })

    expect(() => auth.resolveModel('claude-haiku-9000')).toThrow()
  })
})
