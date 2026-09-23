import axios from 'axios'

import { Error400 } from '@crowd/common'

import { STACKEXCHANGE_CONFIG } from '../../../conf'
import Permissions from '../../../security/permissions'
import track from '../../../segment/track'
import { StackOverflowTagsResponse } from '../../../serverless/integrations/types/stackOverflowTypes'
import PermissionChecker from '../../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHasAny([
    Permissions.values.integrationCreate,
    Permissions.values.integrationEdit,
  ])

  if (req.query.tag) {
    try {
      const result = await axios.get(
        `https://api.stackexchange.com/2.3/tags/${req.query.tag}/info`,
        {
          params: {
            site: 'stackoverflow',
            key: STACKEXCHANGE_CONFIG.key,
          },
        },
      )
      const data = result.data as StackOverflowTagsResponse
      if (
        result.status === 200 &&
        data.items &&
        data.items.length > 0 &&
        data.items[0].is_moderator_only === false &&
        data.items[0].count > 0
      ) {
        track(
          'Stack Overflow: tag input',
          {
            tag: req.query.tag,
            valid: true,
          },
          { ...req },
        )
        return req.responseHandler.success(req, res, data)
      }
    } catch {
      track(
        'Stack Overflow: tag input',
        {
          tag: req.query.tag,
          valid: false,
        },
        { ...req },
      )
      return req.responseHandler.error(req, res, new Error400(req.language))
    }
  }
  track(
    'Stack Overflow: tag input',
    {
      tag: req.query.subreddit,
      valid: false,
    },
    { ...req },
  )
  return req.responseHandler.error(req, res, new Error400(req.language))
}
