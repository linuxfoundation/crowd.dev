import { Transform } from 'stream'

// Shared by npmTarball.ts and goModuleZip.ts — both download third-party archives
// and need the same decompression-bomb guards.
export const FETCH_TIMEOUT_MS = 2 * 60 * 1000
export const MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024
export const MAX_EXTRACTED_BYTES = 500 * 1024 * 1024
export const MAX_EXTRACTED_FILES = 20_000

// counter defaults to a fresh { bytes: 0 } per call (a single download/extraction),
// but callers extracting many entries into one running total (e.g. per zip-entry
// Transforms that can't be reused across pipeline() calls) can pass a shared counter
// object so bytes accumulate across several limiter instances.
export function createDownloadLimiter(
  errorMessage: string,
  maxBytes: number = MAX_DOWNLOAD_BYTES,
  counter: { bytes: number } = { bytes: 0 },
): Transform {
  return new Transform({
    transform(chunk, _encoding, callback) {
      counter.bytes += chunk.length
      if (counter.bytes > maxBytes) {
        callback(new Error(errorMessage))
        return
      }
      callback(null, chunk)
    },
  })
}
