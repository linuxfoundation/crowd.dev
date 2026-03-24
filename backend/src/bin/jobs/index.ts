import { CrowdJob } from '../../types/jobTypes'

import autoImportGroups from './autoImportGroupsioGroups'
import checkStuckIntegrationRuns from './checkStuckIntegrationRuns'
import cleanUp from './cleanUp'
import integrationTicks from './integrationTicks'
import refreshGithubRepoSettingsJob from './refreshGithubRepoSettings'
import refreshGitlabToken from './refreshGitlabToken'
import refreshGroupsioToken from './refreshGroupsioToken'

const jobs: CrowdJob[] = [
  integrationTicks,
  cleanUp,
  checkStuckIntegrationRuns,
  refreshGroupsioToken,
  refreshGitlabToken,
  refreshGithubRepoSettingsJob,
  autoImportGroups,
]

export default jobs
