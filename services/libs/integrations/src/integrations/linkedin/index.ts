import { IIntegrationDescriptor } from '../../types'
import generateStreams from './generateStreams'
import { LINKEDIN_MEMBER_ATTRIBUTES } from './memberAttributes'
import processData from './processData'
import processStream from './processStream'

const descriptor: IIntegrationDescriptor = {
  type: 'linkedin',
  memberAttributes: LINKEDIN_MEMBER_ATTRIBUTES,
  checkEvery: 60,
  generateStreams,
  processStream,
  processData,
}

export default descriptor
