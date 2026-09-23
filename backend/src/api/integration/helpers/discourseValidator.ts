import axios from 'axios'

import { Error400 } from '@crowd/common'

import Permissions from '../../../security/permissions'
import PermissionChecker from '../../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHasAny([
    Permissions.values.integrationCreate,
    Permissions.values.integrationEdit,
  ])

  const { apiKey, apiUsername, forumHostname } = req.body

  if (apiKey && apiUsername && forumHostname) {
    try {
      const result = await axios.get(`${forumHostname}/admin/users/list/active.json`, {
        headers: {
          'Api-Key': apiKey,
          'Api-Username': apiUsername,
        },
      })
      if (result.status === 200 && result.data && result.data.length > 0) {
        return req.responseHandler.success(req, res, result.data)
      }
    } catch {
      return req.responseHandler.error(req, res, new Error400(req.language))
    }
  }
  return req.responseHandler.error(req, res, new Error400(req.language))
}
