// index.ts content
import { PlatformType } from '@crowd/types'

import { IIntegrationDescriptor } from '../../types'
import generateStreams from './generateStreams'
import { GITLAB_MEMBER_ATTRIBUTES } from './memberAttributes'
import processData from './processData'
import processStream from './processStream'
import processWebhookStream from './processWebhookStream'

const descriptor: IIntegrationDescriptor = {
  type: PlatformType.GITLAB,
  memberAttributes: GITLAB_MEMBER_ATTRIBUTES,
  checkEvery: 60,
  generateStreams,
  processStream,
  processWebhookStream,
  processData,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  postProcess: (settings: any) => {
    return settings
  },
}

export default descriptor
