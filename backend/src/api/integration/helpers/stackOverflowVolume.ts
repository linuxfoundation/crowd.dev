import axios from 'axios'

import { Error400 } from '@crowd/common'

import { STACKEXCHANGE_CONFIG } from '../../../conf'
import Permissions from '../../../security/permissions'
import PermissionChecker from '../../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHasAny([
    Permissions.values.integrationCreate,
    Permissions.values.integrationEdit,
  ])

  if (req.query.keywords) {
    try {
      const promises = req.query.keywords.split(';').map((keyword) =>
        axios.get(`https://api.stackexchange.com/2.3/search/advanced`, {
          params: {
            site: 'stackoverflow',
            q: `"${keyword}"`,
            filter: 'total',
            key: STACKEXCHANGE_CONFIG.key,
          },
        }),
      )
      const responses = await Promise.all(promises)
      if (responses.every((response) => response.status === 200)) {
        return req.responseHandler.success(req, res, {
          total: responses.reduce((acc, response) => acc + response.data.total, 0),
        })
      }
    } catch {
      return req.responseHandler.error(req, res, new Error400(req.language))
    }
  }
  return req.responseHandler.error(req, res, new Error400(req.language))
}
