import { defaultPayloadConverter } from '@temporalio/common'
import { decode } from '@temporalio/common/lib/encoding'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { EncryptionCodec } from './encryption-codec'

const OLD_KEY = 'old-key-0123456789abcdef01234567'
const NEW_KEY = 'new-key-fedcba9876543210fedcba98'

async function codec(keyId: string, key: string, previous?: Record<string, string>) {
  vi.stubEnv('CROWD_TEMPORAL_ENCRYPTION_KEY', key)
  vi.stubEnv('CROWD_TEMPORAL_ENCRYPTION_KEYS_PREVIOUS', previous ? JSON.stringify(previous) : '')
  return EncryptionCodec.create(keyId)
}

const payload = () => defaultPayloadConverter.toPayload({ hello: 'world' })

describe('EncryptionCodec', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it('round-trips payloads with the current key', async () => {
    const current = await codec('old', OLD_KEY)
    const [encrypted] = await current.encode([payload()])
    const [decrypted] = await current.decode([encrypted])

    expect(defaultPayloadConverter.fromPayload(decrypted)).toEqual({ hello: 'world' })
  })

  it('decrypts payloads written with a previous key and encrypts new ones with the current key', async () => {
    const [writtenBeforeRotation] = await (await codec('old', OLD_KEY)).encode([payload()])
    const rotated = await codec('new', NEW_KEY, { old: OLD_KEY })

    const [decrypted] = await rotated.decode([writtenBeforeRotation])
    const [writtenAfterRotation] = await rotated.encode([payload()])

    expect(defaultPayloadConverter.fromPayload(decrypted)).toEqual({ hello: 'world' })
    expect(decode(writtenAfterRotation.metadata?.['encryption-key-id'])).toBe('new')
  })

  it('cannot decrypt a payload once its key is no longer configured', async () => {
    const [writtenBeforeRotation] = await (await codec('old', OLD_KEY)).encode([payload()])
    const retired = await codec('new', NEW_KEY)

    await expect(retired.decode([writtenBeforeRotation])).rejects.toThrow()
  })

  it('decrypts unknown key ids with the current key, as before rotation support', async () => {
    const [encrypted] = await (await codec('some-other-id', NEW_KEY)).encode([payload()])
    const current = await codec('new', NEW_KEY)
    const [decrypted] = await current.decode([encrypted])

    expect(defaultPayloadConverter.fromPayload(decrypted)).toEqual({ hello: 'world' })
  })

  it('rejects a malformed previous-keys value without echoing it', async () => {
    vi.stubEnv('CROWD_TEMPORAL_ENCRYPTION_KEY', NEW_KEY)
    vi.stubEnv('CROWD_TEMPORAL_ENCRYPTION_KEYS_PREVIOUS', `old:${OLD_KEY}`)

    const created = EncryptionCodec.create('new')

    await expect(created).rejects.toThrow(
      'CROWD_TEMPORAL_ENCRYPTION_KEYS_PREVIOUS must be a JSON object',
    )
    await expect(created).rejects.not.toThrow(OLD_KEY)
  })
})
