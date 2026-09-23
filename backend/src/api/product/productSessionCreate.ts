import ProductAnalyticsService from '@/services/productAnalyticsService'
import { Error403 } from '@crowd/common'

export default async (req, res) => {
  if (!req.currentUser || !req.currentUser.id) {
    throw new Error403(req.language)
  }

  // cloudflare headers to get the real ip & country
  const ipAddress = req.headers['cf-connecting-ip']
  const country = req.headers['cf-ipcountry']

  req.body = {
    ...req.body,
    ipAddress,
    country,
  }

  const payload = await new ProductAnalyticsService(req).createSession(req.body)

  await req.responseHandler.success(req, res, payload)
}
