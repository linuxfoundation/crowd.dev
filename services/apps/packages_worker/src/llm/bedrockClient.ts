import { BedrockRuntimeClient, InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime'

import type { BedrockCredentials } from '../config'

export type BedrockInvoke = (modelId: string, body: string) => Promise<string>

const clients = new Map<string, BedrockRuntimeClient>()

export function bedrockClientFor(
  region: string,
  credentials: BedrockCredentials,
): BedrockRuntimeClient {
  const key = `${region}:${credentials.accessKeyId}`
  let client = clients.get(key)
  if (!client) {
    client = new BedrockRuntimeClient({ region, credentials })
    clients.set(key, client)
  }
  return client
}

export function invokeBedrockModel(
  region: string,
  credentials: BedrockCredentials,
  timeoutMs: number,
): BedrockInvoke {
  return async (modelId, body) => {
    const controller = new AbortController()
    const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs)
    try {
      const response = await bedrockClientFor(region, credentials).send(
        new InvokeModelCommand({
          modelId,
          accept: 'application/json',
          contentType: 'application/json',
          body,
        }),
        { abortSignal: controller.signal },
      )
      return response.body.transformToString()
    } finally {
      clearTimeout(timeoutHandle)
    }
  }
}
