#!/usr/bin/env tsx

/**
 * Trigger rank_packages() on demand.
 *
 * Usage (from services/apps/packages_worker):
 *   pnpm run:impact
 *   pnpm run:impact --cutoff 0.90
 *   pnpm run:impact --ecosystems npm,go
 *   pnpm run:impact --cutoff 0.85 --ecosystems npm
 */
import { getPackagesDb } from '../db'

process.env.CROWD_PACKAGES_DB_WRITE_HOST ??= 'localhost'
process.env.CROWD_PACKAGES_DB_PORT ??= '5434'
process.env.CROWD_PACKAGES_DB_DATABASE ??= 'packages-db'
process.env.CROWD_PACKAGES_DB_USERNAME ??= 'postgres'
process.env.CROWD_PACKAGES_DB_PASSWORD ??= 'example'

function parseArg(flag: string, fallback: number): number {
  const idx = process.argv.indexOf(flag)
  if (idx === -1) return fallback
  const v = Number(process.argv[idx + 1])
  return Number.isNaN(v) ? fallback : v
}

function parseListArg(flag: string): string[] | null {
  const idx = process.argv.indexOf(flag)
  if (idx === -1) return null
  return process.argv[idx + 1].split(',').map((s) => s.trim())
}

const cutoff = parseArg('--cutoff', 0.9)
const ecosystems = parseListArg('--ecosystems')

async function main() {
  console.log(`Running rank_packages()`)
  console.log(`  cutoff    : ${cutoff}`)
  console.log(`  ecosystems: ${ecosystems ? ecosystems.join(', ') : 'all'}\n`)

  const qx = await getPackagesDb()
  const t = Date.now()

  const [result] = await qx.select(
    `SELECT * FROM rank_packages($/cutoff/, $/ecosystems/)`,
    { cutoff, ecosystems },
  )

  const elapsed = ((Date.now() - t) / 1000).toFixed(1)
  console.log(`Done in ${elapsed}s`)
  console.log(`  processed_rows: ${result.processed_rows?.toLocaleString()}`)

  process.exit(0)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
