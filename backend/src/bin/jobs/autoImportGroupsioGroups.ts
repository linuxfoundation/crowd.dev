import cronGenerator from 'cron-time-generator'

import { getUserSubscriptions } from '@/serverless/integrations/usecases/groupsio/getUserSubscriptions'
import { getServiceChildLogger } from '@crowd/logging'

import SequelizeRepository from '../../database/repositories/sequelizeRepository'
import { CrowdJob } from '../../types/jobTypes'

const log = getServiceChildLogger('autoImportGroupsioGroupsCronJob')

interface SetttingsObj {
  email: string
  token: string
  groups: Array<{
    id: number
    name: string
    slug: string
    groupAddedOn?: Date
  }>
  autoImports?: {
    mainGroup: string
    isAllowed: boolean
  }[]
  password: string
  tokenError: string
  tokenExpiry: string
  updateMemberAttributes: boolean
}

const job: CrowdJob = {
  name: 'Auto Import Groups IO Groups',
  // every 2 days
  cronTime: cronGenerator.every(2).days(),
  onTrigger: async () => {
    log.info('Checking for new groups to auto import.')
    const dbOptions = await SequelizeRepository.getDefaultIRepositoryOptions()

    const integrations = await dbOptions.database.sequelize.query(
      `select id, settings from integrations 
                where  platform = 'groupsio' 
                and "deletedAt" is null        
        `,
    )

    log.info(`Found ${integrations[0].length} integrations to check for auto imports.`)

    for (const integration of integrations[0]) {
      const settings = integration.settings as SetttingsObj
      if (settings.autoImports) {
        const allGroups = await getUserSubscriptions(settings.token)
        log.info(`Found ${allGroups.length} available groups in users's account.`)
        const existingGroupIds = new Set(settings.groups.map((group) => group.id))

        for (const autoImport of settings.autoImports) {
          if (autoImport.isAllowed) {
            const newGroups = allGroups.filter(
              (group) =>
                !existingGroupIds.has(group.id) &&
                group.group_name.startsWith(autoImport.mainGroup),
            )

            for (const newGroup of newGroups) {
              log.info(`Adding new group ${newGroup.nice_group_name} to auto-import.`)
              settings.groups.push({
                id: newGroup.id,
                name: newGroup.nice_group_name,
                slug: newGroup.group_name,
                groupAddedOn: new Date(),
              })
            }

            if (newGroups.length > 0) {
              log.info(
                `Added ${newGroups.length} new groups for auto-import in integration ${integration.id}`,
              )
            } else {
              log.info(`No new groups found for auto-import in integration ${integration.id}.`)
            }
          }
        }

        // Update the integration settings in the database
        await dbOptions.database.integration.update({ settings }, { where: { id: integration.id } })
      }
    }
  },
}

export default job
