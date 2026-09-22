import { GenerateStreamsHandler } from '../../types'
import {
  HackerNewsInitialStreamMetadata,
  HackerNewsIntegrationSettings,
  HackerNewsStreamType,
} from './types'

const handler: GenerateStreamsHandler = async (ctx) => {
  const settings = ctx.integration.settings as HackerNewsIntegrationSettings

  if (settings.keywords.length === 0 && settings.urls.length === 0) {
    await ctx.abortRunWithError('No keywords or urls configured!')
    return
  }

  const keywords = Array.from(new Set([...settings.keywords, ...settings.urls]))

  await ctx.publishStream<HackerNewsInitialStreamMetadata>(HackerNewsStreamType.INITIAL, {
    keywords,
  })
}

export default handler
