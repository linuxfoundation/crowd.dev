import { GenerateStreamsHandler } from '../../types'
import { IRedditIntegrationSettings, IRedditSubredditStreamData, RedditStreamType } from './types'

const handler: GenerateStreamsHandler = async (ctx) => {
  const settings = ctx.integration.settings as IRedditIntegrationSettings

  if (settings.subreddits.length === 0) {
    await ctx.abortRunWithError('No subreddits configured!')
    return
  }

  for (const subreddit of settings.subreddits) {
    await ctx.publishStream<IRedditSubredditStreamData>(
      `${RedditStreamType.SUBREDDIT}:${subreddit}`,
      {
        channel: subreddit,
      },
    )
  }
}

export default handler
