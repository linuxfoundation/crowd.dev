import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'

const log = getServiceChildLogger('rankPackages')

export async function rankPackages(): Promise<void> {
  log.info('Starting packages rank pass')
  const qx = await getPackagesDb()

  const result = await qx.selectOne(`SELECT * FROM rank_packages()`)

  log.info(
    { scored_rows: result.scored_rows, ranked_rows: result.ranked_rows },
    'rank_packages complete',
  )
}
