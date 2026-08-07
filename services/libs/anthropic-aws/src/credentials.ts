export interface AnthropicAwsCredentials {
  region: string
  workspaceId: string
  apiKey: string
}

function requireEnv(name: string): string {
  const value = process.env[name]
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`)
  }
  return value
}

export function getAnthropicAwsCredentials(): AnthropicAwsCredentials {
  return {
    region: requireEnv('CROWD_AKRITES_ANTHROPIC_AWS_REGION'),
    workspaceId: requireEnv('CROWD_AKRITES_ANTHROPIC_AWS_WORKSPACE_ID'),
    apiKey: requireEnv('CROWD_AKRITES_ANTHROPIC_AWS_API_KEY'),
  }
}

// Env vars for Claude Platform on AWS routing; see
// https://code.claude.com/docs/en/claude-platform-on-aws
export function getAnthropicAwsAgentSdkEnv(
  credentials: AnthropicAwsCredentials = getAnthropicAwsCredentials(),
): Record<string, string> {
  return {
    CLAUDE_CODE_USE_ANTHROPIC_AWS: '1',
    ANTHROPIC_AWS_WORKSPACE_ID: credentials.workspaceId,
    AWS_REGION: credentials.region,
    ANTHROPIC_AWS_API_KEY: credentials.apiKey,
  }
}
