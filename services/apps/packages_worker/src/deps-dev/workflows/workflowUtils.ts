/**
 * Split file names into chunks where each chunk's total size is <= bytesPerChunk.
 * Always produces at least one chunk. A single oversized file goes in its own chunk.
 */
export function buildChunks(
  fileNames: string[],
  fileSizes: number[],
  bytesPerChunk: number,
): Array<{ files: string[]; offset: number }> {
  if (fileNames.length === 0) return []
  const chunks: Array<{ files: string[]; offset: number }> = []
  let current: string[] = []
  let currentBytes = 0
  for (let i = 0; i < fileNames.length; i++) {
    current.push(fileNames[i])
    currentBytes += fileSizes[i]
    if (currentBytes >= bytesPerChunk && i < fileNames.length - 1) {
      chunks.push({ files: current, offset: i + 1 - current.length })
      current = []
      currentBytes = 0
    }
  }
  if (current.length > 0) {
    chunks.push({ files: current, offset: fileNames.length - current.length })
  }
  return chunks
}
