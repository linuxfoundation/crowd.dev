import { webcrypto as crypto } from 'node:crypto'

import { METADATA_ENCODING_KEY, Payload, PayloadCodec, ValueError } from '@temporalio/common'
import { decode, encode } from '@temporalio/common/lib/encoding'
import { temporal } from '@temporalio/proto'

import { decrypt, encrypt } from './crypto'

const ENCODING = 'binary/encrypted'
const METADATA_ENCRYPTION_KEY_ID = 'encryption-key-id'

export class EncryptionCodec implements PayloadCodec {
  constructor(
    protected readonly keys: Map<string, crypto.CryptoKey>,
    protected readonly defaultKeyId: string,
  ) {}

  static async create(keyId: string): Promise<EncryptionCodec> {
    const keys = new Map<string, crypto.CryptoKey>()
    for (const [previousKeyId, previousKey] of Object.entries(previousKeys())) {
      keys.set(previousKeyId, await importKey(previousKey))
    }
    keys.set(keyId, await importKey(process.env['CROWD_TEMPORAL_ENCRYPTION_KEY']))
    return new this(keys, keyId)
  }

  async encode(payloads: Payload[]): Promise<Payload[]> {
    return Promise.all(
      payloads.map(async (payload) => ({
        metadata: {
          [METADATA_ENCODING_KEY]: encode(ENCODING),
          [METADATA_ENCRYPTION_KEY_ID]: encode(this.defaultKeyId),
        },
        // Encrypt entire payload, preserving metadata
        data: await encrypt(
          temporal.api.common.v1.Payload.encode(payload).finish(),
          this.keys.get(this.defaultKeyId)!, // eslint-disable-line @typescript-eslint/no-non-null-assertion
        ),
      })),
    )
  }

  async decode(payloads: Payload[]): Promise<Payload[]> {
    return Promise.all(
      payloads.map(async (payload) => {
        if (!payload.metadata || decode(payload.metadata[METADATA_ENCODING_KEY]) !== ENCODING) {
          return payload
        }
        if (!payload.data) {
          throw new ValueError('Payload data is missing')
        }

        const keyIdBytes = payload.metadata[METADATA_ENCRYPTION_KEY_ID]
        if (!keyIdBytes) {
          throw new ValueError('Unable to decrypt Payload without encryption key id')
        }

        const key =this.keys.get(decode(keyIdBytes)) ?? this.keys.get(this.defaultKeyId)
        const decryptedPayloadBytes = await decrypt(payload.data, key)
        return temporal.api.common.v1.Payload.decode(decryptedPayloadBytes)
      }),
    )
  }
}

// The error must never include the raw value: it holds key material.
function previousKeys(): Record<string, string> {
  const raw = process.env['CROWD_TEMPORAL_ENCRYPTION_KEYS_PREVIOUS']
  if (!raw) {
    return {}
  }

  const invalid = new ValueError(
    'CROWD_TEMPORAL_ENCRYPTION_KEYS_PREVIOUS must be a JSON object of key id to non-empty key',
  )
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    throw invalid
  }
  if (
    !parsed ||
    typeof parsed !== 'object' ||
    Array.isArray(parsed) ||
    !Object.values(parsed).every((key) => typeof key === 'string' && key.length > 0)
  ) {
    throw invalid
  }
  return parsed as Record<string, string>
}

async function importKey(key: string): Promise<crypto.CryptoKey> {
  return crypto.subtle.importKey(
    'raw',
    Buffer.from(key),
    {
      name: 'AES-GCM',
    },
    true,
    ['encrypt', 'decrypt'],
  )
}
