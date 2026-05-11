import { CrowdJob } from '../../types/jobTypes'

import autoImportGroups from './autoImportGroupsioGroups'
import cleanUp from './cleanUp'
import integrationTicks from './integrationTicks'
import refreshGithubRepoSettingsJob from './refreshGithubRepoSettings'
import refreshGitlabToken from './refreshGitlabToken'
import refreshGroupsioToken from './refreshGroupsioToken'

const jobs: CrowdJob[] = [
  integrationTicks,
  cleanUp,
  refreshGroupsioToken,
  refreshGitlabToken,
  refreshGithubRepoSettingsJob,
  autoImportGroups,
]

export default jobs
