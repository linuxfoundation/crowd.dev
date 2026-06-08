// GCS objects are covered by a 7-day lifecycle rule on the bucket — no explicit cleanup needed.
// This workflow is reserved for staging-table GC (orphaned tables older than 24h) once
// versioned staging tables are implemented (B4).
export async function cleanupOsspckgs(): Promise<void> {
  // no-op until versioned staging table GC is wired
}
