import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'
import cronGenerator from 'cron-time-generator'

import { GITLAB_CONFIG } from '@/conf'
import { timeout } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'

import SequelizeRepository from '../../database/repositories/sequelizeRepository'
import { CrowdJob } from '../../types/jobTypes'

const log = getServiceChildLogger('refreshGitlabTokenCronJob')

const job: CrowdJob = {
  name: 'Refresh Gitlab token',
  // every hour
  cronTime: cronGenerator.every(1).hours(),
  onTrigger: async () => {
    log.info('Checking Gitlab tokens for refresh.')
    const dbOptions = await SequelizeRepository.getDefaultIRepositoryOptions()

    interface Integration {
      id: string
      token: string
      refreshToken: string
    }

    const gitlabTokens = await dbOptions.database.sequelize.query(
      `SELECT id, token, "refreshToken" FROM integrations 
       WHERE platform = 'gitlab' AND "deletedAt" IS NULL
       AND "createdAt" < NOW() - INTERVAL '1 hour'`,
    )

    for (const integration of gitlabTokens[0] as Integration[]) {
      log.info(`Refreshing token for Gitlab integration: ${integration.id}`)

      try {
        const config: AxiosRequestConfig = {
          method: 'post',
          url: 'https://gitlab.com/oauth/token',
          data: {
            grant_type: 'refresh_token',
            refresh_token: integration.refreshToken,
            client_id: GITLAB_CONFIG.clientId,
            client_secret: GITLAB_CONFIG.clientSecret,
          },
          headers: {
            'Content-Type': 'application/json',
          },
        }

        const response: AxiosResponse = await axios(config)

        const newToken = response.data.access_token
        const newRefreshToken = response.data.refresh_token

        await dbOptions.database.integration.update(
          {
            token: newToken,
            refreshToken: newRefreshToken,
          },
          { where: { id: integration.id } },
        )

        log.info(`Successfully refreshed token for Gitlab integration: ${integration.id}`)
      } catch (err) {
        log.error(`Error refreshing token for Gitlab integration ${integration.id}: ${err.message}`)
      } finally {
        await timeout(1000)
      }
    }
  },
}

export default job
