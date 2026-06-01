import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('rankPackagesUniverse')

export async function rankPackagesUniverse(): Promise<void> {
  const qx = await getPackagesDb()

  // All three steps in one transaction: TRUNCATE+INSERT, rank, copyback.
  // Keeps packages_universe and packages.last_rank_pass_at consistent — if the copyback
  // UPDATE fails the whole tx rolls back rather than leaving the two tables out of sync.
  // M1: pre-aggregate dependent_repos_count via GROUP BY (not correlated subquery per row).
  const { result, rowCount } = await qx.tx(async (tx) => {
    await tx.result(`TRUNCATE packages_universe`)

    await tx.result(`
      INSERT INTO packages_universe (
        purl, ecosystem, namespace, name,
        downloads_last_30d, dependent_packages_count, dependent_repos_count,
        last_rank_pass_at
      )
      SELECT
        p.purl, p.ecosystem, p.namespace, p.name,
        NULL,
        p.dependent_packages_count,
        p.dependent_repos_count,
        NOW()
      FROM packages p
      WHERE p.ecosystem IN ('npm', 'go', 'maven', 'pypi', 'nuget', 'cargo')
    `)

    // Step 2: run criticality ranking function
    const result = await tx.selectOne(`
      SELECT * FROM rank_packages_universe(
        weight_downloads            := 1.0,
        weight_dependent_repos      := 2.0,
        weight_dependent_packages   := 1.5,
        log_smoothing               := 1.0,
        critical_top_n_by_ecosystem := '{
          "npm":   400000,
          "go":    100000,
          "maven": 200000,
          "pypi":  100000,
          "nuget":  50000,
          "cargo":  50000
        }'::jsonb
      )
    `)

    // Step 3: copy is_critical + last_rank_pass_at back to packages
    // rank_packages_universe() does NOT write these columns to packages — do it here.
    const rowCount = await tx.result(`
      UPDATE packages p
      SET
        is_critical       = pu.is_critical,
        last_rank_pass_at = NOW()
      FROM packages_universe pu
      WHERE p.purl = pu.purl
    `)

    return { result, rowCount }
  })

  log.info(
    {
      scored_rows: result.scored_rows,
      ranked_rows: result.ranked_rows,
      propagated_rows: result.propagated_rows,
    },
    'rank_packages_universe complete',
  )

  log.info({ rowCount }, 'packages is_critical + last_rank_pass_at updated')
}
