import { PlatformType } from '@crowd/types'

import { IIntegrationDescriptor } from '../../types'
import generateStreams from './generateStreams'
import { HACKERNEWS_MEMBER_ATTRIBUTES } from './memberAttributes'
import processData from './processData'
import processStream from './processStream'

const descriptor: IIntegrationDescriptor = {
  type: PlatformType.HACKERNEWS,
  memberAttributes: HACKERNEWS_MEMBER_ATTRIBUTES,
  checkEvery: 2 * 60,
  generateStreams,
  processStream,
  processData,
}

export default descriptor
