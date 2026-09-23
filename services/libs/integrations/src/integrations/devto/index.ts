import { PlatformType } from '@crowd/types'

import { IIntegrationDescriptor } from '../../types'
import generateStreams from './generateStreams'
import { DEVTO_MEMBER_ATTRIBUTES } from './memberAttributes'
import processData from './processData'
import processStream from './processStream'

const descriptor: IIntegrationDescriptor = {
  type: PlatformType.DEVTO,
  memberAttributes: DEVTO_MEMBER_ATTRIBUTES,
  checkEvery: 30,

  generateStreams,
  processStream,
  processData,
  processWebhookStream: async (ctx) => {
    console.log('webhook stream data', ctx.stream.type, ctx.stream.identifier, ctx.stream.data)
  },
}

export default descriptor
