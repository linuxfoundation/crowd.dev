import { WRITE_DB_CONFIG, getDbConnection, pgpQx } from '@crowd/data-access-layer/src/database'

import { createDailyProjectOnboardingCap } from '../../api/public/v1/projectOnboarding/dailyRequestCap'

export const reserveDailyProjectOnboardingRequest = createDailyProjectOnboardingCap()

// optionsBgQx routes QueryTypes.SELECT to the read replica, which breaks our
// INSERT/UPDATE ... RETURNING claims — pin this detached workflow to the writer instead.
export async function getBgQx() {
  const db = await getDbConnection(WRITE_DB_CONFIG())
  return pgpQx(db)
}
