import { describe, expect, it } from 'vitest'

import { createDownloadLimiter } from '../downloadLimits'

function collect(transform: ReturnType<typeof createDownloadLimiter>): Promise<void> {
  return new Promise((resolve, reject) => {
    transform.on('data', () => undefined)
    transform.on('end', resolve)
    transform.on('error', reject)
  })
}

describe('createDownloadLimiter', () => {
  it('passes chunks through while under the threshold', async () => {
    const limiter = createDownloadLimiter('exceeded', 1024)
    const done = collect(limiter)
    limiter.end(Buffer.alloc(512, 'x'))
    await expect(done).resolves.toBeUndefined()
  })

  it('errors once cumulative bytes exceed an injected threshold', async () => {
    const limiter = createDownloadLimiter('exceeded size limit', 1024)
    const done = collect(limiter)
    limiter.write(Buffer.alloc(600, 'x'))
    limiter.end(Buffer.alloc(600, 'x'))
    await expect(done).rejects.toThrow('exceeded size limit')
  })
})
