import { safeWrap } from '@/middlewares/errorMiddleware'
import { NANGO_CLOUD_CONFIG, getNangoCloudSessionToken, initNangoCloudClient } from '@crowd/nango'

export default async (app) => {
  if (NANGO_CLOUD_CONFIG()) {
    await initNangoCloudClient()
    app.get(
      '/nango/session',
      safeWrap(async (req, res) => {
        const data = await getNangoCloudSessionToken()
        await req.responseHandler.success(req, res, data)
      }),
    )
  }
}
