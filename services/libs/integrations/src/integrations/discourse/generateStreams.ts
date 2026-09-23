// generateStreams.ts content
import { GenerateStreamsHandler } from '../../types'
import { DiscourseCategoryStreamData, DiscourseStreamType } from './types'

const handler: GenerateStreamsHandler = async (ctx) => {
  await ctx.publishStream<DiscourseCategoryStreamData>(DiscourseStreamType.CATEGORIES, {
    page: '',
  })
}

export default handler
