import { PlatformType } from '@crowd/types'

import { IIntegrationDescriptor } from '../../types'
import generateStreams from './generateStreams'
import { STACKOVERFLOW_MEMBER_ATTRIBUTES } from './memberAttributes'
import processData from './processData'
import processStream from './processStream'

const descriptor: IIntegrationDescriptor = {
  type: PlatformType.STACKOVERFLOW,
  memberAttributes: STACKOVERFLOW_MEMBER_ATTRIBUTES,
  checkEvery: 360, // 6 hours
  generateStreams,
  processStream,
  processData,
}

export default descriptor
