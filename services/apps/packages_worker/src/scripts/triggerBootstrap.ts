import { TEMPORAL_CONFIG, getTemporalClient } from '@crowd/temporal'

import { bootstrapOsspckgs } from '../deps-dev/workflows'

const VALID_KINDS = [
  'packages',
  'versions',
  'package_dependencies',
  'repos',
  'package_repos',
  'advisories',
  'advisory_packages',
  'dependent_counts',
  'scorecard',
] as const

const HELP = `
Usage: trigger-bootstrap [full|incremental] [ECOSYSTEMS] [options]

Arguments:
  full | incremental     Sync mode (default: full)
  ECOSYSTEMS             Comma-separated list: CARGO,NPM,MAVEN,GO,PYPI,NUGET
                         Omit for all 6 ecosystems

Options:
  --kinds <k1,k2>        Run only specific job kinds (default: all). Comma-separated:
                           ${VALID_KINDS.join(', ')}
                         "repos" and "package_repos" always run together (one workflow).
                         "advisories" and "advisory_packages" always run together (one workflow).
  --snapshot-date DATE   Override BQ snapshot resolution for all partition-filtered kinds.
                         Use YYYY-MM-DD. Skips resolveSnapshotDate BQ call and uses this date
                         directly. Useful for re-running with a known-good older snapshot.
  --reuse-exports        Skip BQ for any kind that has recent exported data in DB+GCS.
                         Falls back to BQ if files are gone or no prior export exists.
  --export-name <name>   Skip BQ entirely for kinds covered by a named export
                         (created via export-to-bucket). More explicit than --reuse-exports.
  --deps-table-b         Use DependenciesLatest instead of DependencyGraphEdgesLatest
                         for deps. Cheaper (~$4.69 vs $12.67 for CARGO) but loses
                         version_constraint. ADR-0003 Option B. Good for local testing.
  --help                 Show this help

Examples:
  pnpm trigger-bootstrap:local full
  pnpm trigger-bootstrap:local full CARGO
  pnpm trigger-bootstrap:local full CARGO,NPM
  pnpm trigger-bootstrap:local incremental CARGO
  pnpm trigger-bootstrap:local full CARGO --reuse-exports
  pnpm trigger-bootstrap:local full CARGO --deps-table-b
  pnpm trigger-bootstrap:local full CARGO --export-name cargo-may-2026
  pnpm trigger-bootstrap:local full CARGO --export-name cargo-may-2026 --deps-table-b
  pnpm trigger-bootstrap:local full --kinds dependent_counts
  pnpm trigger-bootstrap:local full --kinds packages,versions
  pnpm trigger-bootstrap:local full --kinds dependent_counts --snapshot-date 2026-06-04
`

async function main(): Promise<void> {
  const args = process.argv.slice(2)

  if (args.includes('--help') || args.includes('-h')) {
    console.log(HELP)
    process.exit(0)
  }

  const reuseExports = args.includes('--reuse-exports')
  const depsTableOption: 'A' | 'B' = args.includes('--deps-table-b') ? 'B' : 'A'

  const exportNameIdx = args.indexOf('--export-name')
  if (
    exportNameIdx !== -1 &&
    (exportNameIdx + 1 >= args.length || args[exportNameIdx + 1].startsWith('--'))
  ) {
    console.error('--export-name requires a value')
    process.exit(1)
  }
  const exportName = exportNameIdx !== -1 ? args[exportNameIdx + 1] : undefined

  const snapshotDateIdx = args.indexOf('--snapshot-date')
  if (
    snapshotDateIdx !== -1 &&
    (snapshotDateIdx + 1 >= args.length || args[snapshotDateIdx + 1].startsWith('--'))
  ) {
    console.error('--snapshot-date requires a value (YYYY-MM-DD)')
    process.exit(1)
  }
  const snapshotDate = snapshotDateIdx !== -1 ? args[snapshotDateIdx + 1] : undefined
  if (snapshotDate && !/^\d{4}-\d{2}-\d{2}$/.test(snapshotDate)) {
    console.error(`--snapshot-date must be YYYY-MM-DD, got: ${snapshotDate}`)
    process.exit(1)
  }

  const kindsIdx = args.indexOf('--kinds')
  if (kindsIdx !== -1 && (kindsIdx + 1 >= args.length || args[kindsIdx + 1].startsWith('--'))) {
    console.error('--kinds requires a value')
    process.exit(1)
  }
  let kinds: string[] | undefined
  if (kindsIdx !== -1) {
    kinds = args[kindsIdx + 1].split(',').map((k) => k.trim())
    const invalid = kinds.filter((k) => !VALID_KINDS.includes(k as (typeof VALID_KINDS)[number]))
    if (invalid.length > 0) {
      console.error(`Unknown kind(s): ${invalid.join(', ')}. Valid: ${VALID_KINDS.join(', ')}`)
      process.exit(1)
    }
  }

  const positional: string[] = []
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      if (args[i] === '--export-name' || args[i] === '--kinds' || args[i] === '--snapshot-date') i++ // skip value
      continue
    }
    positional.push(args[i])
  }

  const mode = (positional[0] ?? 'full') as 'full' | 'incremental'
  if (mode !== 'full' && mode !== 'incremental') {
    console.error(`Unknown mode "${mode}". Use "full" or "incremental".`)
    process.exit(1)
  }

  const ecosystems = positional[1]
    ? positional[1].split(',').map((e) => e.trim().toUpperCase())
    : undefined

  const VALID_ECOSYSTEMS = new Set(['NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO'])
  if (ecosystems) {
    const invalid = ecosystems.filter((e) => !VALID_ECOSYSTEMS.has(e))
    if (invalid.length > 0) {
      console.error(
        `Unknown ecosystem(s): ${invalid.join(', ')}. Valid: ${[...VALID_ECOSYSTEMS].join(', ')}`,
      )
      process.exit(1)
    }
  }

  const cfg = TEMPORAL_CONFIG()
  if (!cfg.serverUrl || !cfg.namespace) {
    console.error('Missing CROWD_TEMPORAL_SERVER_URL or CROWD_TEMPORAL_NAMESPACE')
    process.exit(1)
  }

  const client = await getTemporalClient(cfg)

  const ecosystemSuffix = ecosystems ? `-${ecosystems.join('-').toLowerCase()}` : ''
  const reuseSuffix = reuseExports ? '-reuse' : ''
  const tableSuffix = depsTableOption === 'B' ? '-depsB' : ''
  const workflowId = `bootstrap-osspckgs-${mode}${ecosystemSuffix}${reuseSuffix}${tableSuffix}-${Date.now()}`
  const handle = await client.workflow.start(bootstrapOsspckgs, {
    taskQueue: 'bq-dataset-ingest',
    workflowId,
    args: [{ mode, ecosystems, kinds, reuseExports, depsTableOption, exportName, snapshotDate }],
  })

  const flags = [
    kinds ? `--kinds ${kinds.join(',')}` : '',
    snapshotDate ? `--snapshot-date ${snapshotDate}` : '',
    reuseExports ? '--reuse-exports' : '',
    depsTableOption === 'B' ? '--deps-table-b' : '',
    exportName ? `--export-name ${exportName}` : '',
  ].filter(Boolean)
  console.log(
    `Started workflow ${handle.workflowId}${ecosystems ? ` (ecosystems: ${ecosystems.join(', ')})` : ''}${flags.length ? ` [${flags.join(' ')}]` : ''}`,
  )
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Failed to trigger bootstrap:', err)
    process.exit(1)
  })
