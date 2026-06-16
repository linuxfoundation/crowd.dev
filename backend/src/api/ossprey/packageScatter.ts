import { listPackagesForScatter } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'

export default async (req, res) => {
  const qx = await getPackagesQx()
  const points = await listPackagesForScatter(qx)

  await req.responseHandler.success(req, res, {
    points,
    total: points.length,
  })
}
