#!/usr/bin/env tsx
/**
 * Trigger rank_packages_universe() on demand.
 *
 * Usage (from services/apps/packages_worker):
 *   pnpm run:impact
 *   pnpm run:impact --top-n '{"cargo":75000,"maven":150000}'
 *   pnpm run:impact --w-downloads 0.30 --w-dep-pkgs 0.20 --w-transitive 0.50
 */

import { getPackagesDb } from '../db'

// Env defaults for local dev
process.env.CROWD_PACKAGES_DB_WRITE_HOST ??= 'localhost'
process.env.CROWD_PACKAGES_DB_PORT       ??= '5434'
process.env.CROWD_PACKAGES_DB_DATABASE   ??= 'packages-db'
process.env.CROWD_PACKAGES_DB_USERNAME   ??= 'postgres'
process.env.CROWD_PACKAGES_DB_PASSWORD   ??= 'example'

function parseArg(flag: string, fallback: number): number {
  const idx = process.argv.indexOf(flag)
  return idx !== -1 ? Number(process.argv[idx + 1]) || fallback : fallback
}

function parseJsonArg(flag: string, fallback: string): string {
  const idx = process.argv.indexOf(flag)
  return idx !== -1 ? process.argv[idx + 1] : fallback
}

const wDownloads  = parseArg('--w-downloads', 0.25)
const wDepPkgs    = parseArg('--w-dep-pkgs',  0.25)
const wTransitive = parseArg('--w-transitive', 0.50)
const topN        = parseJsonArg('--top-n', '{"cargo":75000,"maven":150000}')

async function main() {
  if (Math.abs(wDownloads + wDepPkgs + wTransitive - 1.0) > 0.001) {
    console.error(`Weights must sum to 1.0, got ${(wDownloads + wDepPkgs + wTransitive).toFixed(3)}`)
    process.exit(1)
  }

  console.log(`Running rank_packages_universe()`)
  console.log(`  weights: downloads=${wDownloads}  dep_pkgs=${wDepPkgs}  transitive=${wTransitive}`)
  console.log(`  top-n  : ${topN}\n`)

  const qx = await getPackagesDb()
  const t   = Date.now()

  const [result] = await qx.select(
    `SELECT * FROM rank_packages_universe($/wDownloads/, $/wDepPkgs/, $/wTransitive/, $/topN/::jsonb)`,
    { wDownloads, wDepPkgs, wTransitive, topN },
  )

  const elapsed = ((Date.now() - t) / 1000).toFixed(1)
  console.log(`Done in ${elapsed}s`)
  console.log(`  scored_rows    : ${result.scored_rows?.toLocaleString()}`)
  console.log(`  ranked_rows    : ${result.ranked_rows?.toLocaleString()}`)
  console.log(`  propagated_rows: ${result.propagated_rows?.toLocaleString()}`)

  process.exit(0)
}

main().catch(err => { console.error(err); process.exit(1) })
