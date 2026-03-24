// index.ts content
import { PlatformType } from '@crowd/types'

import { IIntegrationDescriptor } from '../../types'

import generateStreams from './generateStreams'
import { GITHUB_MEMBER_ATTRIBUTES } from './memberAttributes'
import processData from './processData'
import processStream from './processStream'
import processWebhookStream from './processWebhookStream'

const descriptor: IIntegrationDescriptor = {
  type: PlatformType.GITHUB,
  memberAttributes: GITHUB_MEMBER_ATTRIBUTES,
  generateStreams,
  processStream,
  processWebhookStream,
  processData,
}

export default descriptor
