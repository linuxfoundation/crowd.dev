import { Transform } from 'stream'

// Shared by npmTarball.ts and goModuleZip.ts — both download third-party archives
// and need the same decompression-bomb guards.
export const FETCH_TIMEOUT_MS = 2 * 60 * 1000
export const MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024
export const MAX_EXTRACTED_BYTES = 500 * 1024 * 1024
export const MAX_EXTRACTED_FILES = 20_000

export function createDownloadLimiter(errorMessage: string): Transform {
  let downloadedBytes = 0
  return new Transform({
    transform(chunk, _encoding, callback) {
      downloadedBytes += chunk.length
      if (downloadedBytes > MAX_DOWNLOAD_BYTES) {
        callback(new Error(errorMessage))
        return
      }
      callback(null, chunk)
    },
  })
}
