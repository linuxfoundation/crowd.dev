import { getOsspreyMetrics } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'

export default async (req, res) => {
  const qx = await getPackagesQx()
  const metrics = await getOsspreyMetrics(qx)
  await req.responseHandler.success(req, res, metrics)
}
