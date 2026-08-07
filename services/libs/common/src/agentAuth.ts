export type AgentAuthMode = 'bedrock' | 'anthropic-api-key' | 'cli-fallback'

export interface AgentAuth {
  mode: AgentAuthMode
  env: Record<string, string> | undefined
  resolveModel(model: string): string
}

export interface BedrockEnvVarNames {
  accessKeyId: string
  secretAccessKey: string
  region: string
}

export interface ResolveAgentAuthOptions {
  apiKeyEnvVar?: string
  modelBedrockMap?: Record<string, string>
  // Which env vars carry the Bedrock credential for this caller. There is no single
  // org-wide Bedrock credential: CROWD_AWS_BEDROCK_* (the default) is the one shared by
  // the enrichment workers, unrelated to Akrites. Callers under Akrites must pass their
  // own AKRITES_* var names explicitly rather than relying on this default.
  bedrockEnvVarNames?: BedrockEnvVarNames
}

const DEFAULT_BEDROCK_ENV_VAR_NAMES: BedrockEnvVarNames = {
  accessKeyId: 'CROWD_AWS_BEDROCK_ACCESS_KEY_ID',
  secretAccessKey: 'CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY',
  region: 'CROWD_AWS_BEDROCK_REGION',
}

const DEFAULT_BEDROCK_REGION = 'us-east-1'

// The CLI prefers ANTHROPIC_API_KEY over Bedrock env vars when both are present in the
// subprocess env, so it must be stripped explicitly — otherwise Bedrock mode silently
// never activates even with valid AWS credentials.
function bedrockEnv(accessKeyId: string, secretAccessKey: string, region: string) {
  const env = { ...process.env } as Record<string, string>
  delete env.ANTHROPIC_API_KEY
  env.CLAUDE_CODE_USE_BEDROCK = '1'
  env.AWS_ACCESS_KEY_ID = accessKeyId
  env.AWS_SECRET_ACCESS_KEY = secretAccessKey
  env.AWS_REGION = region
  return env
}

function resolveModelWith(modelBedrockMap: Record<string, string> | undefined) {
  return (model: string): string => {
    if (!modelBedrockMap) {
      return model
    }
    const resolved = modelBedrockMap[model]
    if (!resolved) {
      throw new Error(`No Bedrock model ID mapped for agent model "${model}"`)
    }
    return resolved
  }
}

export function resolveAgentAuth(opts: ResolveAgentAuthOptions = {}): AgentAuth {
  const {
    apiKeyEnvVar = 'BLAST_RADIUS_ANTHROPIC_API_KEY',
    modelBedrockMap,
    bedrockEnvVarNames = DEFAULT_BEDROCK_ENV_VAR_NAMES,
  } = opts

  const bedrockAccessKeyId = process.env[bedrockEnvVarNames.accessKeyId]
  const bedrockSecretAccessKey = process.env[bedrockEnvVarNames.secretAccessKey]

  if (bedrockAccessKeyId && bedrockSecretAccessKey) {
    const region = process.env[bedrockEnvVarNames.region] || DEFAULT_BEDROCK_REGION
    return {
      mode: 'bedrock',
      env: bedrockEnv(bedrockAccessKeyId, bedrockSecretAccessKey, region),
      resolveModel: resolveModelWith(modelBedrockMap),
    }
  }

  const apiKey = process.env[apiKeyEnvVar]
  if (apiKey) {
    const baseUrl = process.env.BLAST_RADIUS_ANTHROPIC_BASE_URL
    return {
      mode: 'anthropic-api-key',
      env: {
        ...process.env,
        ANTHROPIC_API_KEY: apiKey,
        ...(baseUrl ? { ANTHROPIC_BASE_URL: baseUrl } : {}),
      } as Record<string, string>,
      resolveModel: (model) => model,
    }
  }

  return {
    mode: 'cli-fallback',
    env: undefined,
    resolveModel: (model) => model,
  }
}
