#!/usr/bin/env tsx

/**
 * Standalone PageRank runner / graph validator.
 *
 * Usage (from services/apps/packages_worker):
 *   pnpm run:pagerank [ecosystem]          ← full run
 *   pnpm run:pagerank cargo --graph-only   ← build + validate graph, skip iterations
 */
import { getPackagesDb } from '../db'

import { criticalityComputePageRank } from './activities'
import { buildGraph, getDependents } from './graph'
import { loadDirectEdges } from './queries'

// Env defaults for local dev
process.env.CROWD_PACKAGES_DB_WRITE_HOST ??= 'localhost'
process.env.CROWD_PACKAGES_DB_PORT ??= '5434'
process.env.CROWD_PACKAGES_DB_DATABASE ??= 'packages-db'
process.env.CROWD_PACKAGES_DB_USERNAME ??= 'postgres'
process.env.CROWD_PACKAGES_DB_PASSWORD ??= 'example'

const ecosystem = process.argv[2] ?? 'cargo'
const graphOnly = process.argv.includes('--graph-only')

const SPOT_CHECK: Record<string, string[]> = {
  cargo: ['tokio', 'serde', 'log', 'rand', 'clap', 'libc', 'anyhow', 'syn', 'quote', 'proc-macro2'],
  maven: [
    'junit',
    'guava',
    'slf4j-api',
    'commons-lang3',
    'spring-core',
    'jackson-databind',
    'log4j',
    'mockito-core',
    'commons-io',
    'logback-classic',
  ],
  npm: [
    'react',
    'lodash',
    'express',
    'typescript',
    'axios',
    'webpack',
    'babel-core',
    'jest',
    'eslint',
    'chalk',
  ],
}

async function main() {
  const qx = await getPackagesDb()

  // ── Build graph ───────────────────────────────────────────────────────────
  console.log(`Building graph for ecosystem=${ecosystem} ...`)
  let t = Date.now()
  const edges = await loadDirectEdges(qx, ecosystem)
  const edgeCount = edges.length
  const graph = buildGraph(edges)
  edges.length = 0
  console.log(
    `  ${graph.N.toLocaleString()} nodes  ${edgeCount.toLocaleString()} edges  (${((Date.now() - t) / 1000).toFixed(1)}s)\n`,
  )

  // ── Spot-check dependents against DB ─────────────────────────────────────
  console.log('Spot-checking dependents against DB ...')
  let passed = 0,
    failed = 0

  for (const name of SPOT_CHECK[ecosystem] ?? []) {
    const [pkg] = await qx.select(
      `SELECT id FROM packages WHERE name = $/name/ AND ecosystem = $/ecosystem/ LIMIT 1`,
      { name, ecosystem },
    )
    if (!pkg) {
      console.log(`  -  ${name.padEnd(20)} not found`)
      continue
    }

    const dbRows: Array<{ package_id: number }> = await qx.select(
      `SELECT DISTINCT pd.package_id
         FROM package_dependencies pd
         JOIN packages p ON p.id = pd.package_id AND p.ecosystem = $/ecosystem/
        WHERE pd.depends_on_id = $/id/ AND pd.dependency_kind = 'direct'`,
      { ecosystem, id: pkg.id },
    )

    const dbSet = new Set(dbRows.map((r) => r.package_id))
    const csrSet = new Set(getDependents(graph, pkg.id))
    const ok = dbSet.size === csrSet.size && [...dbSet].every((id) => csrSet.has(id))

    if (ok) {
      console.log(
        `  ✓  ${name.padEnd(20)} ${dbSet.size.toLocaleString().padStart(6)} dependents match`,
      )
      passed++
    } else {
      const missing = [...dbSet].filter((id) => !csrSet.has(id)).length
      const extra = [...csrSet].filter((id) => !dbSet.has(id)).length
      console.log(`  ✗  ${name.padEnd(20)} MISMATCH  missing=${missing}  extra=${extra}`)
      failed++
    }
  }

  console.log(`\n${failed === 0 ? '✓' : '✗'}  ${passed}/${passed + failed} spot-checks passed`)

  if (graphOnly || failed > 0) process.exit(failed > 0 ? 1 : 0)

  // ── Full PageRank ─────────────────────────────────────────────────────────
  console.log('\nRunning PageRank ...')
  t = Date.now()
  const result = await criticalityComputePageRank({ ecosystem })
  console.log(
    `  nodes=${result.nodeCount.toLocaleString()}  edges=${result.edgeCount.toLocaleString()}  iters=${result.iterations}  duration=${(result.durationMs / 1000).toFixed(1)}s`,
  )

  process.exit(0)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
