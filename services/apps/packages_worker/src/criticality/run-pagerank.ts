#!/usr/bin/env tsx
/**
 * Standalone PageRank runner / graph validator.
 *
 * Usage (from services/apps/packages_worker):
 *   pnpm dev:pagerank [ecosystem]          ← full run
 *   pnpm dev:pagerank cargo --graph-only   ← build + validate graph, skip iterations
 */

import * as fs from 'node:fs'
import { getPackagesDb } from '../db'
import { buildGraph, getDependents } from './graph'
import { loadDirectEdges } from './queries'
import { criticalityComputePageRank } from './activities'

// Env defaults for local dev
process.env.CROWD_PACKAGES_DB_WRITE_HOST ??= 'localhost'
process.env.CROWD_PACKAGES_DB_PORT       ??= '5434'
process.env.CROWD_PACKAGES_DB_DATABASE   ??= 'packages-db'
process.env.CROWD_PACKAGES_DB_USERNAME   ??= 'postgres'
process.env.CROWD_PACKAGES_DB_PASSWORD   ??= 'example'

const ecosystem  = process.argv[2] ?? 'cargo'
const graphOnly  = process.argv.includes('--graph-only')

const SPOT_CHECK: Record<string, string[]> = {
  cargo: ['tokio', 'serde', 'log', 'rand', 'clap', 'libc', 'anyhow', 'syn', 'quote', 'proc-macro2'],
  maven: ['junit', 'guava', 'slf4j-api', 'commons-lang3', 'spring-core', 'jackson-databind', 'log4j', 'mockito-core', 'commons-io', 'logback-classic'],
  npm:   ['react', 'lodash', 'express', 'typescript', 'axios', 'webpack', 'babel-core', 'jest', 'eslint', 'chalk'],
}

declare const gc: (() => void) | undefined

function forceGc() { if (typeof gc === 'function') gc() }

function memRaw() {
  const m = process.memoryUsage()
  return { heapMb: m.heapUsed / 1024 / 1024, buffersMb: m.arrayBuffers / 1024 / 1024, rssMb: m.rss / 1024 / 1024 }
}

function mem() {
  const { heapMb, buffersMb, rssMb } = memRaw()
  const mb = (n: number) => `${n.toFixed(1)} MB`
  return { heap: mb(heapMb), buffers: mb(buffersMb), rss: mb(rssMb) }
}

function printMemRow(label: string, durationMs: number | null) {
  const { heap, buffers, rss } = mem()
  const dur = durationMs !== null ? `${(durationMs / 1000).toFixed(1)}s`.padStart(6) : '      '
  console.log(`  ${label.padEnd(26)} ${dur}    heap=${heap.padStart(8)}  buffers=${buffers.padStart(8)}  rss=${rss.padStart(8)}`)
}

async function main() {
  const qx = await getPackagesDb()
  const report: Record<string, unknown> = { ecosystem, timestamp: new Date().toISOString(), phases: [] }
  const phases = report.phases as Array<Record<string, unknown>>

  const snap = (label: string, durationMs: number | null) => {
    const s = { label, durationMs, ...memRaw() }
    phases.push(s)
    return s
  }

  console.log(`Building graph for ecosystem=${ecosystem}\n`)
  console.log(`  ${'phase'.padEnd(26)} ${'dur'.padStart(6)}    ${'heap'.padStart(13)}  ${'buffers'.padStart(17)}  ${'rss'.padStart(12)}`)
  console.log(`  ${'─'.repeat(80)}`)
  snap('initial', null); printMemRow('initial', null)

  // ── Step 1: load edges ────────────────────────────────────────────────────
  let t = Date.now()
  const edges = await loadDirectEdges(qx, ecosystem)
  const edgeCount = edges.length
  const loadMs = Date.now() - t
  snap(`edges loaded`, loadMs); printMemRow(`edges loaded (${edgeCount.toLocaleString()})`, loadMs)

  // ── Step 2: build CSR ─────────────────────────────────────────────────────
  t = Date.now()
  const graph = buildGraph(edges)
  edges.length = 0
  forceGc()
  const csrMs = Date.now() - t
  snap('CSR built, edges freed', csrMs); printMemRow('CSR built, edges freed', csrMs)

  report.nodeCount = graph.N
  report.edgeCount = edgeCount
  console.log()

  // ── Spot-check dependents against DB ─────────────────────────────────────
  console.log('Spot-checking dependents against DB ...')
  let passed = 0, failed = 0

  const checkNames = SPOT_CHECK[ecosystem] ?? []
  for (const name of checkNames) {
    const [pkg] = await qx.select(
      `SELECT id FROM packages WHERE name = $/name/ AND ecosystem = $/ecosystem/ LIMIT 1`,
      { name, ecosystem },
    )

    if (!pkg) { console.log(`  -  ${name.padEnd(20)} not found`); continue }

    const dbRows: Array<{ package_id: number }> = await qx.select(
      `SELECT DISTINCT pd.package_id
         FROM package_dependencies pd
         JOIN packages p ON p.id = pd.package_id AND p.ecosystem = $/ecosystem/
        WHERE pd.depends_on_id = $/id/ AND pd.dependency_kind = 'direct'`,
      { ecosystem, id: pkg.id },
    )

    const dbSet  = new Set(dbRows.map(r => r.package_id))
    const csrSet = new Set(getDependents(graph, pkg.id))

    const ok = dbSet.size === csrSet.size &&
               [...dbSet].every(id => csrSet.has(id))

    if (ok) {
      console.log(`  ✓  ${name.padEnd(20)} ${dbSet.size.toLocaleString().padStart(6)} dependents match`)
      passed++
    } else {
      const missing = [...dbSet].filter(id => !csrSet.has(id)).length
      const extra   = [...csrSet].filter(id => !dbSet.has(id)).length
      console.log(`  ✗  ${name.padEnd(20)} MISMATCH  missing=${missing}  extra=${extra}`)
      failed++
    }
  }

  console.log(`\n${failed === 0 ? '✓' : '✗'}  ${passed}/${passed+failed} spot-checks passed`)

  if (graphOnly || failed > 0) {
    process.exit(failed > 0 ? 1 : 0)
  }

  // ── Full PageRank ─────────────────────────────────────────────────────────
  console.log('Running full PageRank (re-loads edges internally) ...\n')
  console.log(`  ${'phase'.padEnd(26)} ${'dur'.padStart(6)}    ${'heap'.padStart(13)}  ${'buffers'.padStart(17)}  ${'rss'.padStart(12)}`)
  console.log(`  ${'─'.repeat(80)}`)
  forceGc()
  snap('before PageRank call', null); printMemRow('before PageRank call', null)

  t = Date.now()
  const result = await criticalityComputePageRank({ ecosystem })
  forceGc()
  const prMs = Date.now() - t
  snap('after PageRank + DB write', prMs); printMemRow('after PageRank + DB write', prMs)

  report.pageRank = {
    nodes: result.nodeCount,
    edges: result.edgeCount,
    iterations: result.iterations,
    durationMs: result.durationMs,
  }

  console.log(`\n  nodes=${result.nodeCount.toLocaleString()}  edges=${result.edgeCount.toLocaleString()}  iters=${result.iterations}  duration=${(result.durationMs/1000).toFixed(1)}s`)

  // ── Save report ───────────────────────────────────────────────────────────
const outFile = `pagerank-report-${ecosystem}-${Date.now()}.json`
  fs.writeFileSync(outFile, JSON.stringify(report, null, 2))
  console.log(`\nReport saved → ${outFile}`)

  process.exit(0)
}

main().catch(err => { console.error(err); process.exit(1) })
