import { PlatformType } from '@crowd/types'

import { IIntegrationDescriptor } from '../../types'

import generateStreams from './generateStreams'
import { DISCORD_MEMBER_ATTRIBUTES } from './memberAttributes'
import processData from './processData'
import processStream from './processStream'

const descriptor: IIntegrationDescriptor = {
  type: PlatformType.DISCORD,
  memberAttributes: DISCORD_MEMBER_ATTRIBUTES,
  generateStreams,
  processStream,
  processData,
}

export default descriptor
