import axios from 'axios'

import { IProcessStreamContext } from '../../../types'
import { handleGitlabError } from './errorHandler'

export const refreshToken = async (ctx: IProcessStreamContext) => {
  const platformSettings = ctx.platformSettings as {
    clientId: string
    clientSecret: string
  }

  try {
    const response = await axios.post('https://gitlab.com/oauth/token', {
      grant_type: 'refresh_token',
      refresh_token: ctx.integration.refreshToken,
      client_id: platformSettings.clientId,
      client_secret: platformSettings.clientSecret,
    })

    return response.data
  } catch (error) {
    throw handleGitlabError(error, 'refreshToken', ctx.log)
  }
}
