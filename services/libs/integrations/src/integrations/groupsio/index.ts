// index.ts content
import { PlatformType } from '@crowd/types'

import { IIntegrationDescriptor } from '../../types'
import generateStreams from './generateStreams'
import { GROUPSIO_MEMBER_ATTRIBUTES } from './memberAttributes'
import processData from './processData'
import processStream from './processStream'
import processWebhookStream from './processWebhookStream'

const descriptor: IIntegrationDescriptor = {
  type: PlatformType.GROUPSIO,
  memberAttributes: GROUPSIO_MEMBER_ATTRIBUTES,
  generateStreams,
  processStream,
  processData,
  checkEvery: 2 * 24 * 60,
  processWebhookStream,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  postProcess: (settings: any) => {
    return settings
  },
}

export default descriptor
