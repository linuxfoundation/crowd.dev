import { PlatformType } from '@crowd/types'

import { GithubActivityType } from './github/types'
import { HackerNewsActivityType } from './hackernews/types'
import { RedditActivityType } from './reddit/types'

export const prettyActivityTypes = {
  [PlatformType.GITHUB]: {
    [GithubActivityType.FORK]: 'forked',
    // literal keys: GithubActivityType.STAR/UNSTAR are gone (CM-1439), but old persisted activities still carry these
    star: 'starred',
    unstar: 'unstarred',
    'pull_request-open': 'opened a new pull request',
    [GithubActivityType.PULL_REQUEST_OPENED]: 'opened a new pull request',
    'pull_request-close': 'closed a pull request',
    [GithubActivityType.PULL_REQUEST_CLOSED]: 'closed a pull request',
    'issues-open': 'opened a new issue',
    [GithubActivityType.ISSUE_OPENED]: 'opened a new issue',
    'issues-close': 'closed an issue',
    [GithubActivityType.ISSUE_CLOSED]: 'closed an issue',
    [GithubActivityType.ISSUE_COMMENT]: 'commented on an issue',
    [GithubActivityType.PULL_REQUEST_COMMENT]: 'commented on a pull request',
    [GithubActivityType.DISCUSSION_STARTED]: 'started a discussion',
    [GithubActivityType.DISCUSSION_COMMENT]: 'commented on a discussion',
    contributed_to_community: 'contributed to community',
    joined_community: 'joined community',
  },
  [PlatformType.DISCORD]: {
    contributed_to_community: 'contributed to community',
    joined_community: 'joined community',
    message: 'sent a message',
    replied: 'replied to a message',
    replied_thread: 'replied to a thread',
    thread_started: 'started a new thread',
    started_thread: 'started a new thread',
    joined_guild: 'joined server',
  },
  [PlatformType.SLACK]: {
    contributed_to_community: 'contributed to community',
    joined_community: 'joined community',
    message: 'sent a message',
    replied: 'replied to a message',
    replied_thread: 'replied to a thread',
    file_share: 'shared a file',
    reaction_added: 'reacted to a message',
    channel_joined: 'joined channel',
    left_channel: 'left channel',
  },
  [PlatformType.TWITTER]: {
    contributed_to_community: 'contributed to community',
    joined_community: 'joined community',
    mention: 'mentioned you in a tweet',
    hashtag: 'posted a tweet',
    follow: 'followed you',
  },
  [PlatformType.DEVTO]: {
    comment: 'comment',
  },
  [PlatformType.HACKERNEWS]: {
    [HackerNewsActivityType.POST]: 'posted',
    [HackerNewsActivityType.COMMENT]: 'commented ',
  },
  [PlatformType.REDDIT]: {
    [RedditActivityType.POST]: 'posted',
    [RedditActivityType.COMMENT]: 'commented',
  },
  [PlatformType.LINKEDIN]: {
    comment: 'commented',
    reaction: 'reacted',
  },
}
