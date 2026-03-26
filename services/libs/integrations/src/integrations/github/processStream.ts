/* eslint-disable  @typescript-eslint/no-explicit-any */
// processStream.ts content
import { createAppAuth } from '@octokit/auth-app'
import { AuthInterface } from '@octokit/auth-app/dist-types/types'

import { singleOrDefault, timeout } from '@crowd/common'
import { GraphQlQueryResponse, IConcurrentRequestLimiter } from '@crowd/types'

import {
  IProcessStreamContext,
  IProcessWebhookStreamContext,
  ProcessStreamHandler,
} from '../../types'

import DiscussionCommentsQuery from './api/graphql/discussionComments'
import DiscussionsQuery from './api/graphql/discussions'
import ForksQuery from './api/graphql/forks'
import IssueCommentsQuery from './api/graphql/issueComments'
import IssuesQuery from './api/graphql/issues'
import getMember from './api/graphql/members'
import getOrganization from './api/graphql/organizations'
import PullRequestCommentsQuery from './api/graphql/pullRequestComments'
import PullRequestCommitsQuery, { PullRequestCommit } from './api/graphql/pullRequestCommits'
import PullRequestCommitsQueryNoAdditions, {
  PullRequestCommitNoAdditions,
} from './api/graphql/pullRequestCommitsNoAdditions'
import PullRequestReviewThreadCommentsQuery from './api/graphql/pullRequestReviewThreadComments'
import PullRequestReviewThreadsQuery from './api/graphql/pullRequestReviewThreads'
import PullRequestsQuery from './api/graphql/pullRequests'
import StargazersQuery from './api/graphql/stargazers'
import { GithubTokenRotator } from './tokenRotator'
import {
  GithubActivitySubType,
  GithubActivityType,
  GithubApiData,
  GithubBasicStream,
  GithubBotMember,
  GithubIntegrationSettings,
  GithubPlatformSettings,
  GithubPrepareMemberOutput,
  GithubPrepareOrgMemberOutput,
  GithubPullRequestEvents,
  GithubRootStream,
  GithubStreamType,
  INDIRECT_FORK,
  Repo,
  Repos,
} from './types'

const IS_TEST_ENV: boolean = process.env.NODE_ENV === 'test'

const containsHrefAttribute = (htmlSnippet: string) => {
  // Constructing the regex using RegExp
  const hrefRegex = new RegExp('href\\s*=\\s*["\'][^"\']*["\']', 'i')
  return hrefRegex.test(htmlSnippet)
}

let githubAuthenticator: AuthInterface | undefined = undefined
let concurrentRequestLimiter: IConcurrentRequestLimiter | undefined = undefined
let tokenRotator: GithubTokenRotator | undefined = undefined

function getAuth(ctx: IProcessStreamContext): AuthInterface | undefined {
  const GITHUB_CONFIG = ctx.platformSettings as GithubPlatformSettings
  const privateKey = GITHUB_CONFIG.privateKey
    ? Buffer.from(GITHUB_CONFIG.privateKey, 'base64').toString('ascii')
    : undefined

  if (githubAuthenticator === undefined) {
    githubAuthenticator = privateKey
      ? createAppAuth({
          appId: GITHUB_CONFIG.appId,
          clientId: GITHUB_CONFIG.clientId,
          clientSecret: GITHUB_CONFIG.clientSecret,
          privateKey,
        })
      : undefined
  }
  return githubAuthenticator
}

export function getConcurrentRequestLimiter(
  ctx: IProcessStreamContext | IProcessWebhookStreamContext,
): IConcurrentRequestLimiter {
  if (concurrentRequestLimiter === undefined) {
    concurrentRequestLimiter = ctx.getConcurrentRequestLimiter(
      50, // max 2 concurrent requests
      'github-concurrent-request-limiter',
    )
  }
  return concurrentRequestLimiter
}

export function getTokenRotator(ctx: IProcessStreamContext): GithubTokenRotator {
  const GITHUB_CONFIG = ctx?.platformSettings as GithubPlatformSettings

  // check if we have tokens configured
  if (!GITHUB_CONFIG?.personalAccessTokens) {
    // if tokenRotator is undefined, API requests won't use it
    return undefined
  }

  if (tokenRotator === undefined) {
    tokenRotator = new GithubTokenRotator(
      ctx.globalCache,
      GITHUB_CONFIG?.personalAccessTokens?.split(','),
    )
  }
  return tokenRotator
}

export async function getGithubToken(ctx: IProcessStreamContext): Promise<string> {
  const auth = getAuth(ctx)
  if (auth) {
    const authResponse = await auth({
      type: 'installation',
      installationId: ctx.integration.identifier,
    })
    const token = authResponse.token
    // this is a real token, not a JWT token
    // OctoKit automatically caches it for 1 hour and renews it when needed
    return token
  }

  throw new Error('GitHub integration is not configured!')
}

async function getMemberData(ctx: IProcessStreamContext, login: string): Promise<any> {
  const appToken = await getGithubToken(ctx)
  return getMember(login, appToken, getTokenRotator(ctx), {
    concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
    integrationId: ctx.integration.id,
  })
}

async function getOrganizationData(ctx: IProcessStreamContext, company: string): Promise<any> {
  if (company === '' || company === null || company === undefined) {
    return null
  }

  const cache = ctx.globalCache
  const prefix = (x: string) => `github-org:${x}`

  const existing = await cache.get(prefix(company))
  if (existing) {
    if (existing === 'null') {
      return null
    }

    return JSON.parse(existing)
  }

  const token = await getGithubToken(ctx)
  const fromAPI = await getOrganization(company, token, getTokenRotator(ctx), {
    concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
    integrationId: ctx.integration.id,
  })

  if (fromAPI) {
    await cache.set(prefix(company), JSON.stringify(fromAPI), 24 * 60 * 60)
    return fromAPI
  }

  await cache.set(prefix(company), 'null', 24 * 60 * 60)
  return null
}

async function getMemberEmail(ctx: IProcessStreamContext, login: string): Promise<string> {
  if (IS_TEST_ENV) {
    return ''
  }

  // here we use global cache - it is shared between all integrations
  // So in LFX case different integration will have access to the same cache
  // But this is fine
  const cache = ctx.globalCache
  const prefix = (x: string) => `github-login:${x}`

  const existing = await cache.get(prefix(login))
  if (existing) {
    if (existing === 'null') {
      return ''
    }

    return existing
  }

  const member = await getMemberData(ctx, login)
  const email = (member && member.email ? member.email : '').trim()
  if (email && email.length > 0) {
    await cache.set(prefix(login), email, 24 * 60 * 60)
    return email
  }

  await cache.set(prefix(login), 'null', 24 * 60 * 60)
  return ''
}

const publishNextPageStream = async (
  ctx: IProcessStreamContext,
  response: GraphQlQueryResponse,
) => {
  const data = ctx.stream.data as GithubBasicStream
  // the last part of stream identifier is page number (e.g commits:12345:1)
  const streamIdentifier = ctx.stream.identifier.split(':').slice(0, -1).join(':')
  if (response.hasPreviousPage) {
    await ctx.publishStream<GithubBasicStream>(`${streamIdentifier}:${response.startCursor}`, {
      repo: data.repo,
      page: response.startCursor,
    })
  }
}

// this function extracts email and orgs from member data
export const prepareMember = async (
  memberFromApi: any,
  ctx: IProcessStreamContext,
): Promise<GithubPrepareMemberOutput> => {
  const email = await getMemberEmail(ctx, memberFromApi.login)

  let orgs: any

  if (memberFromApi?.company) {
    if (IS_TEST_ENV) {
      orgs = [{ name: 'crowd.dev' }]
    } else {
      const companyHTML = memberFromApi?.companyHTML
      // if company is matched againts github org it's html will contain href attribute
      if (companyHTML && containsHrefAttribute(companyHTML)) {
        const company = memberFromApi.company.replace('@', '').trim()
        const fromAPI = await getOrganizationData(ctx, company)
        orgs = fromAPI
      } else {
        orgs = null
      }
    }
  }

  return {
    email,
    orgs,
    memberFromApi,
  }
}

export const prepareBotMember = (bot: GithubBotMember): GithubPrepareMemberOutput => {
  return {
    email: '',
    orgs: [],
    memberFromApi: {
      login: bot.login,
      avatarUrl: bot?.avatarUrl || bot?.avatar_url || '',
      url: bot.url,
      id: bot.id,
      isBot: true,
    },
  }
}

export const prepareDeletedMember = (): GithubPrepareMemberOutput => {
  return {
    email: '',
    orgs: [],
    memberFromApi: {
      login: 'ghost',
      avatarUrl: 'https://avatars.githubusercontent.com/u/10137?v=4',
      url: 'https://github.com/ghost',
      isDeleted: true,
    },
  }
}

export const prepareMemberFromOrg = (orgFromApi: any): GithubPrepareOrgMemberOutput => {
  return {
    orgFromApi,
  }
}

/**
 * Searches given repository name among installed repositories
 * Returns null if given repo is not found.
 * @param name  The tenant we are working on
 * @param context
 * @returns Found repo object
 */
function getRepoByName(name: string, ctx: IProcessStreamContext): Repo | null {
  const settings = ctx.integration.settings as GithubIntegrationSettings
  const availableRepo: Repo | undefined = singleOrDefault(
    settings?.orgs?.flatMap((o) => o.repos || []),
    (r) => r.name === name,
  )
  if (availableRepo) {
    return { ...availableRepo, available: true }
  }

  const unavailableRepo: Repo | undefined = singleOrDefault(
    settings?.unavailableRepos,
    (r) => r.name === name,
  )
  if (unavailableRepo) {
    return { ...unavailableRepo, available: false }
  }

  return null
}

const processRootStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubRootStream
  const repos: Repos = []
  const unavailableRepos: Repos = []

  for (const repo of data.reposToCheck) {
    try {
      // we don't need to get default 100 item per page, just 1 is enough to check if repo is available
      const stargazersQuery = new StargazersQuery(repo, await getGithubToken(ctx), 1)
      await stargazersQuery.getSinglePage('')
      repos.push(repo)
    } catch (e) {
      if (e.rateLimitResetSeconds) {
        throw e
      } else {
        ctx.log.warn(
          `Repo ${repo.name} will not be parsed. It is not available with the github token`,
        )
        console.log('e', e)
        unavailableRepos.push(repo)
      }
    }
  }

  // Only persist unavailableRepos for next-run re-checking.
  // Repos live in the repositories table — do not write them back to settings.
  await ctx.updateIntegrationSettings({
    unavailableRepos,
  })

  // now it's time to start streams
  for (const repo of repos) {
    for (const endpoint of [
      GithubStreamType.STARGAZERS,
      GithubStreamType.FORKS,
      GithubStreamType.PULLS,
      GithubStreamType.ISSUES,
      GithubStreamType.DISCUSSIONS,
    ]) {
      // this firstPage thing is important to avoid duplicate streams and for handleNextPageStream to work
      await ctx.publishStream<GithubBasicStream>(`${endpoint}:${repo.name}:firstPage`, {
        repo,
        page: '',
      })
    }
  }
}

const processStargazersStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const stargazersQuery = new StargazersQuery(data.repo, await getGithubToken(ctx))

  const result = await stargazersQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )
  // handle next page
  await publishNextPageStream(ctx, result)

  for (const record of result.data) {
    if (record.node === null) {
      throw new Error(
        'Stargazer is not found. This might be a deleted user. Please check the data.',
      )
    }

    const member = await prepareMember(record.node, ctx)

    // publish data
    await ctx.processData<GithubApiData>({
      type: GithubActivityType.STAR,
      data: record,
      member,
      repo: data.repo,
      isOld: true,
    })
  }
}

const processForksStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const forksQuery = new ForksQuery(data.repo, await getGithubToken(ctx))
  const result = await forksQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )

  // handle next page
  await publishNextPageStream(ctx, result)

  for (const record of result.data) {
    if (record.owner === null) {
      throw new Error('Fork owner is not found. This might be a deleted user.')
    }
    if (record.owner.__typename === 'User') {
      const member = await prepareMember(record.owner, ctx)

      // publish data
      await ctx.processData<GithubApiData>({
        type: GithubActivityType.FORK,
        data: record,
        member,
        repo: data.repo,
        isOld: true,
      })
    } else if (record.owner.__typename === 'Organization') {
      const orgMember = prepareMemberFromOrg(record.owner)

      // publish data
      await ctx.processData<GithubApiData>({
        type: GithubActivityType.FORK,
        data: record,
        orgMember,
        repo: data.repo,
        isOld: true,
      })
    } else {
      ctx.log.warn(`Unsupported owner type: ${record.owner.__typename}`)
    }

    // traverse through indirect forks
    for (const indirectFork of record.indirectForks.nodes) {
      if (indirectFork.owner === null) {
        throw new Error('Fork owner is not found. This might be a deleted user.')
      }
      if (indirectFork.owner.__typename === 'User') {
        const member = await prepareMember(indirectFork.owner, ctx)

        // publish data
        await ctx.processData<GithubApiData>({
          type: GithubActivityType.FORK,
          subType: INDIRECT_FORK,
          data: indirectFork,
          relatedData: record,
          member,
          repo: data.repo,
          isOld: true,
        })
      } else if (indirectFork.owner.__typename === 'Organization') {
        const orgMember = prepareMemberFromOrg(indirectFork.owner)

        // publish data
        await ctx.processData<GithubApiData>({
          type: GithubActivityType.FORK,
          subType: INDIRECT_FORK,
          data: indirectFork,
          relatedData: record,
          orgMember,
          repo: data.repo,
          isOld: true,
        })
      } else {
        ctx.log.warn(`Unsupported owner type: ${indirectFork.owner.__typename}`)
      }
    }
  }
}

const processPullsStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const forksQuery = new PullRequestsQuery(data.repo, await getGithubToken(ctx))
  const result = await forksQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )

  // handle next page
  await publishNextPageStream(ctx, result)

  for (const pull of result.data) {
    let member: GithubPrepareMemberOutput
    if (pull.author?.login) {
      member = await prepareMember(pull.author, ctx)
    } else if (pull.authorBot?.login) {
      member = prepareBotMember(pull.authorBot)
    } else if (pull.author === null && pull.authorBot === null) {
      member = prepareDeletedMember()
    } else {
      ctx.log.warn('Pull request author is not found. This pull request will not be parsed.')
      continue
    }

    // publish data
    await ctx.processData<GithubApiData>({
      type: GithubActivityType.PULL_REQUEST_OPENED,
      data: pull,
      member,
      repo: data.repo,
      isOld: true,
    })

    // now we need to parse pullRequestEvents
    for (const pullEvent of pull.timelineItems.nodes) {
      switch (pullEvent.__typename) {
        case GithubPullRequestEvents.ASSIGN: {
          let member: GithubPrepareMemberOutput
          let objectMember: GithubPrepareMemberOutput

          if (pullEvent?.actor?.login) {
            member = await prepareMember(pullEvent.actor, ctx)
          } else if (pullEvent?.actorBot?.login) {
            member = prepareBotMember(pullEvent.actorBot)
          } else if (pullEvent.actor === null && pullEvent.actorBot === null) {
            member = prepareDeletedMember()
          } else {
            ctx.log.warn(
              'Pull request author is not found. This pull request event will not be parsed.',
            )
            continue
          }

          if (pullEvent?.assignee?.login) {
            objectMember = await prepareMember(pullEvent.assignee, ctx)
          } else if (pullEvent?.assigneeBot?.login) {
            objectMember = prepareBotMember(pullEvent.assigneeBot)
          } else if (pullEvent.assignee === null && pullEvent.assigneeBot === null) {
            objectMember = prepareDeletedMember()
          } else {
            ctx.log.warn(
              'Pull request assignee is not found. This pull request assignee event will not be parsed.',
            )
            continue
          }

          await ctx.processData<GithubApiData>({
            type: GithubActivityType.PULL_REQUEST_ASSIGNED,
            data: pullEvent,
            relatedData: pull,
            member,
            objectMember,
            repo: data.repo,
            isOld: true,
          })

          break
        }
        case GithubPullRequestEvents.REQUEST_REVIEW: {
          // Requested review from single member
          if (pullEvent?.requestedReviewer?.login || pullEvent?.requestedReviewerBot?.login) {
            let member: GithubPrepareMemberOutput
            let objectMember: GithubPrepareMemberOutput

            if (pullEvent?.actor?.login) {
              member = await prepareMember(pullEvent.actor, ctx)
            } else if (pullEvent?.actorBot?.login) {
              member = prepareBotMember(pullEvent.actorBot)
            } else if (pullEvent.actor === null && pullEvent.actorBot === null) {
              member = prepareDeletedMember()
            } else {
              ctx.log.warn(
                'Pull request author is not found. This pull request event will not be parsed.',
              )
              continue
            }

            if (pullEvent?.requestedReviewer?.login) {
              objectMember = await prepareMember(pullEvent.requestedReviewer, ctx)
            } else if (pullEvent?.requestedReviewerBot?.login) {
              objectMember = prepareBotMember(pullEvent.requestedReviewerBot)
            } else if (
              pullEvent.requestedReviewer === null &&
              pullEvent.requestedReviewerBot === null
            ) {
              objectMember = prepareDeletedMember()
            } else {
              ctx.log.warn(
                'Pull request requested reviewer is not found. This pull request requested reviewer event will not be parsed.',
              )
              continue
            }
            await ctx.processData<GithubApiData>({
              type: GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED,
              data: pullEvent,
              subType: GithubActivitySubType.PULL_REQUEST_REVIEW_REQUESTED_SINGLE,
              relatedData: pull,
              member,
              objectMember,
              repo: data.repo,
              isOld: true,
            })
            // Requested review from multiple members
          } else if (pullEvent?.requestedReviewer?.members) {
            let member: GithubPrepareMemberOutput
            if (pullEvent?.actor?.login) {
              member = await prepareMember(pullEvent.actor, ctx)
            } else if (pullEvent?.actorBot?.login) {
              member = prepareBotMember(pullEvent.actorBot)
            } else if (pullEvent.actor === null && pullEvent.actorBot === null) {
              member = prepareDeletedMember()
            } else {
              ctx.log.warn(
                'Pull request author is not found. This pull request event will not be parsed.',
              )
              continue
            }

            for (const teamMember of pullEvent.requestedReviewer.members.nodes) {
              const objectMember = await prepareMember(teamMember, ctx)
              await ctx.processData<GithubApiData>({
                type: GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED,
                data: pullEvent,
                subType: GithubActivitySubType.PULL_REQUEST_REVIEW_REQUESTED_MULTIPLE,
                relatedData: pull,
                member,
                objectMember,
                repo: data.repo,
                isOld: true,
              })
            }
          }

          break
        }
        case GithubPullRequestEvents.REVIEW: {
          if ((pullEvent?.author?.login || pullEvent?.authorBot?.login) && pullEvent?.submittedAt) {
            let member: GithubPrepareMemberOutput
            if (pullEvent?.author?.login) {
              member = await prepareMember(pullEvent.author, ctx)
            } else if (pullEvent?.authorBot?.login) {
              member = prepareBotMember(pullEvent.authorBot)
            } else if (pullEvent.author === null && pullEvent.authorBot === null) {
              member = prepareDeletedMember()
            } else {
              ctx.log.warn(
                'Pull request author is not found. This pull request event will not be parsed.',
              )
              continue
            }

            await ctx.processData<GithubApiData>({
              type: GithubActivityType.PULL_REQUEST_REVIEWED,
              data: pullEvent,
              relatedData: pull,
              member,
              repo: data.repo,
              isOld: true,
            })
          }
          break
        }
        case GithubPullRequestEvents.MERGE: {
          let member: GithubPrepareMemberOutput
          if (pullEvent?.actor?.login) {
            member = await prepareMember(pullEvent.actor, ctx)
          } else if (pullEvent?.actorBot?.login) {
            member = prepareBotMember(pullEvent.actorBot)
          } else if (pullEvent.actor === null && pullEvent.actorBot === null) {
            member = prepareDeletedMember()
          } else {
            ctx.log.warn(
              'Pull request author is not found. This pull request event will not be parsed.',
            )
            continue
          }

          await ctx.processData<GithubApiData>({
            type: GithubActivityType.PULL_REQUEST_MERGED,
            data: pullEvent,
            relatedData: pull,
            member,
            repo: data.repo,
            isOld: true,
          })

          break
        }
        case GithubPullRequestEvents.CLOSE: {
          let member: GithubPrepareMemberOutput
          if (pullEvent?.actor?.login) {
            member = await prepareMember(pullEvent.actor, ctx)
          } else if (pullEvent?.actorBot?.login) {
            member = prepareBotMember(pullEvent.actorBot)
          } else if (pullEvent.actor === null && pullEvent.actorBot === null) {
            member = prepareDeletedMember()
          } else {
            ctx.log.warn(
              'Pull request author is not found. This pull request event will not be parsed.',
            )
            continue
          }

          await ctx.processData<GithubApiData>({
            type: GithubActivityType.PULL_REQUEST_MERGED,
            data: pullEvent,
            relatedData: pull,
            member,
            repo: data.repo,
            isOld: true,
          })

          break
        }
        default:
          ctx.log.warn(
            `Unsupported pull request event:  ${pullEvent.__typename}. This event will not be parsed.`,
          )
      }
    }
  }

  const GITHUB_CONFIG = ctx.platformSettings as GithubPlatformSettings
  const IS_GITHUB_COMMIT_DATA_ENABLED = GITHUB_CONFIG.isCommitDataEnabled === 'true'

  if (IS_GITHUB_COMMIT_DATA_ENABLED) {
    // publish new pull commits streams
    // It is very important to keep commits first. Otherwise, we have problems
    // creating conversations if the Git integration has already ran for those data points.
    for (const pull of result.data) {
      await ctx.publishStream<GithubBasicStream>(
        `${GithubStreamType.PULL_COMMITS}:${pull.number}:firstPage`,
        {
          repo: data.repo,
          page: '',
          prNumber: pull.number,
        },
      )
    }
  }

  // publish new pull comments streams
  for (const pull of result.data) {
    await ctx.publishStream<GithubBasicStream>(
      `${GithubStreamType.PULL_COMMENTS}:${pull.number}:firstPage`,
      {
        repo: data.repo,
        page: '',
        prNumber: pull.number,
      },
    )
  }

  // publish new pull review threads streams
  for (const pull of result.data) {
    await ctx.publishStream<GithubBasicStream>(
      `${GithubStreamType.PULL_REVIEW_THREADS}:${pull.number}:firstPage`,
      {
        repo: data.repo,
        page: '',
        prNumber: pull.number,
      },
    )
  }
}

const processPullCommentsStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const pullRequestNumber = data.prNumber
  const pullRequestCommentsQuery = new PullRequestCommentsQuery(
    data.repo,
    pullRequestNumber,
    await getGithubToken(ctx),
  )

  const result = await pullRequestCommentsQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )

  // handle next page
  await publishNextPageStream(ctx, result)

  // get member info for each comment
  for (const record of result.data) {
    let member: GithubPrepareMemberOutput
    if (record.author?.login) {
      member = await prepareMember(record.author, ctx)
    } else if (record.authorBot?.login) {
      member = prepareBotMember(record.authorBot)
    } else if (record.author === null && record.authorBot === null) {
      member = prepareDeletedMember()
    } else {
      ctx.log.warn(
        'Pull request comment author is not found. This pull request comment will not be parsed.',
      )
      continue
    }

    // publish data
    await ctx.processData<GithubApiData>({
      type: GithubActivityType.PULL_REQUEST_COMMENT,
      data: record,
      member,
      repo: data.repo,
      isOld: true,
    })
  }
}

const processPullReviewThreadsStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const pullRequestNumber = data.prNumber
  const pullRequestReviewThreadsQuery = new PullRequestReviewThreadsQuery(
    data.repo,
    pullRequestNumber,
    await getGithubToken(ctx),
  )

  const result = await pullRequestReviewThreadsQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )

  // handle next page
  await publishNextPageStream(ctx, result)

  // no data to publish, just add new streams for comments inside
  // add each review thread as separate stream for comments inside
  for (const thread of result.data) {
    await ctx.publishStream<GithubBasicStream>(
      `${GithubStreamType.PULL_REVIEW_THREAD_COMMENTS}:${thread.id}:firstPage`,
      {
        repo: data.repo,
        page: '',
        reviewThreadId: thread.id,
      },
    )
  }
}

const processPullReviewThreadCommentsStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const reviewThreadId = data.reviewThreadId
  const pullRequestReviewThreadCommentsQuery = new PullRequestReviewThreadCommentsQuery(
    data.repo,
    reviewThreadId,
    await getGithubToken(ctx),
  )

  const result = await pullRequestReviewThreadCommentsQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )

  // handle next page
  await publishNextPageStream(ctx, result)

  // get additional member info for each comment
  for (const record of result.data) {
    let member: GithubPrepareMemberOutput
    if (record.author?.login) {
      member = await prepareMember(record.author, ctx)
    } else if (record.authorBot?.login) {
      member = prepareBotMember(record.authorBot)
    } else if (record.author === null && record.authorBot === null) {
      member = prepareDeletedMember()
    } else {
      ctx.log.warn(
        'Pull request review thread comment author is not found. This pull request review thread comment will not be parsed.',
      )
      continue
    }

    // publish data
    await ctx.processData<GithubApiData>({
      type: GithubActivityType.PULL_REQUEST_REVIEW_THREAD_COMMENT,
      data: record,
      member,
      repo: data.repo,
      isOld: true,
    })
  }
}

export const processPullCommitsStream: ProcessStreamHandler = async (ctx) => {
  let result: GraphQlQueryResponse

  const data = ctx.stream.data as GithubBasicStream
  const pullRequestNumber = data.prNumber

  const token = await getGithubToken(ctx)

  const pullRequestCommitsQuery = new PullRequestCommitsQuery(data.repo, pullRequestNumber, token)

  try {
    result = await pullRequestCommitsQuery.getSinglePage(
      data.page,
      {
        concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
        integrationId: ctx.integration.id,
      },
      getTokenRotator(ctx),
    )
  } catch (err) {
    ctx.log.warn(
      {
        err,
        repo: data.repo,
        pullRequestNumber,
      },
      'Error while fetching pull request commits. Trying again without additions.',
    )
    const pullRequestCommitsQueryNoAdditions = new PullRequestCommitsQueryNoAdditions(
      data.repo,
      pullRequestNumber,
      await getGithubToken(ctx),
    )
    result = await pullRequestCommitsQueryNoAdditions.getSinglePage(
      data.page,
      {
        concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
        integrationId: ctx.integration.id,
      },
      getTokenRotator(ctx),
    )
  }

  // handle next page
  await publishNextPageStream(ctx, result)

  // getting additional member info for each commit
  {
    const response = result.data[0] as PullRequestCommit | PullRequestCommitNoAdditions
    const commits = response.repository.pullRequest.commits.nodes

    for (const record of commits) {
      for (const author of record.commit.authors.nodes) {
        if (!author || !author?.user || !author?.user?.login) {
          // eslint-disable-next-line no-continue
          continue
        }
        const member = await prepareMember(author.user, ctx)

        // publish data
        await ctx.processData<GithubApiData>({
          type: GithubActivityType.AUTHORED_COMMIT,
          data: record,
          sourceParentId: response.repository.pullRequest.id,
          member,
          repo: data.repo,
          isOld: true,
        })
      }
    }
  }
}

const processIssuesStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const issuesQuery = new IssuesQuery(data.repo, await getGithubToken(ctx))
  const result = await issuesQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )

  // handle next page
  await publishNextPageStream(ctx, result)

  for (const issue of result.data) {
    let member: GithubPrepareMemberOutput
    if (issue.author?.login) {
      member = await prepareMember(issue.author, ctx)
    } else if (issue.authorBot?.login) {
      member = prepareBotMember(issue.authorBot)
    } else if (issue.author === null && issue.authorBot === null) {
      member = prepareDeletedMember()
    } else {
      ctx.log.warn('Issue author is not found. This issue will not be parsed.')
      continue
    }

    await ctx.processData<GithubApiData>({
      type: GithubActivityType.ISSUE_OPENED,
      data: issue,
      member,
      repo: data.repo,
      isOld: true,
    })

    // now need to parse issue events
    for (const issueEvent of issue.timelineItems.nodes) {
      switch (issueEvent.__typename) {
        case GithubPullRequestEvents.CLOSE: {
          let member: GithubPrepareMemberOutput
          if (issueEvent.actor?.login) {
            member = await prepareMember(issueEvent.actor, ctx)
          } else if (issueEvent.actorBot?.login) {
            member = prepareBotMember(issueEvent.actorBot)
          } else if (issueEvent.actor === null && issueEvent.actorBot === null) {
            member = prepareDeletedMember()
          } else {
            ctx.log.warn('Issue event author is not found. This issue event will not be parsed.')
            continue
          }
          await ctx.processData<GithubApiData>({
            type: GithubActivityType.ISSUE_CLOSED,
            data: issueEvent,
            relatedData: issue,
            member,
            repo: data.repo,
            isOld: true,
          })

          break
        }
        default:
          ctx.log.warn(
            `Unsupported issue event:  ${issueEvent.__typename}. This event will not be parsed.`,
          )
      }
    }
  }

  // add each issue as separate stream for comments inside
  for (const issue of result.data) {
    await ctx.publishStream<GithubBasicStream>(
      `${GithubStreamType.ISSUE_COMMENTS}:${issue.number}:firstPage`,
      {
        repo: data.repo,
        page: '',
        issueNumber: issue.number,
      },
    )
  }
}

const processIssueCommentsStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const issueNumber = data.issueNumber
  const issueCommentsQuery = new IssueCommentsQuery(
    data.repo,
    issueNumber,
    await getGithubToken(ctx),
  )
  const result = await issueCommentsQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )
  // handle next page
  await publishNextPageStream(ctx, result)

  // get additional member info for each comment
  for (const record of result.data) {
    let member: GithubPrepareMemberOutput
    if (record.author?.login) {
      member = await prepareMember(record.author, ctx)
    } else if (record.authorBot?.login) {
      member = prepareBotMember(record.authorBot)
    } else if (record.author === null && record.authorBot === null) {
      member = prepareDeletedMember()
    } else {
      ctx.log.warn('Issue comment author is not found. This issue comment will not be parsed.')
      continue
    }

    // publish data
    await ctx.processData<GithubApiData>({
      type: GithubActivityType.ISSUE_COMMENT,
      data: record,
      member,
      repo: data.repo,
      isOld: true,
    })
  }
}

const processDiscussionsStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const discussionsQuery = new DiscussionsQuery(data.repo, await getGithubToken(ctx))
  const result = await discussionsQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )
  // handle next page
  await publishNextPageStream(ctx, result)

  // get additional member info for each comment
  for (const discussion of result.data) {
    let member: GithubPrepareMemberOutput
    if (discussion.author?.login) {
      member = await prepareMember(discussion.author, ctx)
    } else if (discussion.authorBot?.login) {
      member = prepareBotMember(discussion.authorBot)
    } else if (discussion.author === null && discussion.authorBot === null) {
      member = prepareDeletedMember()
    } else {
      ctx.log.warn('Discussion author is not found. This discussion will not be parsed.')
      continue
    }

    // publish data
    await ctx.processData<GithubApiData>({
      type: GithubActivityType.DISCUSSION_STARTED,
      data: discussion,
      member,
      repo: data.repo,
      isOld: true,
    })
  }

  // add each discussion as separate stream for comments inside
  for (const d of result.data.filter((d) => d.comments.totalCount > 0)) {
    await ctx.publishStream<GithubBasicStream>(
      `${GithubStreamType.DISCUSSION_COMMENTS}:${d.id}:firstPage`,
      {
        repo: data.repo,
        page: '',
        discussionNumber: d.number,
      },
    )
  }
}

const processDiscussionCommentsStream: ProcessStreamHandler = async (ctx) => {
  const data = ctx.stream.data as GithubBasicStream
  const discussionCommentsQuery = new DiscussionCommentsQuery(
    data.repo,
    data.discussionNumber,
    await getGithubToken(ctx),
  )
  const result = await discussionCommentsQuery.getSinglePage(
    data.page,
    {
      concurrentRequestLimiter: getConcurrentRequestLimiter(ctx),
      integrationId: ctx.integration.id,
    },
    getTokenRotator(ctx),
  )
  // handle next page
  await publishNextPageStream(ctx, result)

  for (const record of result.data) {
    let member: GithubPrepareMemberOutput
    if (record.author?.login) {
      member = await prepareMember(record.author, ctx)
    } else if (record.authorBot?.login) {
      member = prepareBotMember(record.authorBot)
    } else if (record.author === null && record.authorBot === null) {
      member = prepareDeletedMember()
    } else {
      ctx.log.warn(
        'Discussion comment author is not found. This discussion comment will not be parsed.',
      )
      continue
    }
    const commentId = record.id

    // publish data
    await ctx.processData<GithubApiData>({
      type: GithubActivityType.DISCUSSION_COMMENT,
      subType: GithubActivitySubType.DISCUSSION_COMMENT_START,
      data: record,
      member,
      repo: data.repo,
      isOld: true,
    })

    // going through replies
    for (const reply of record.replies.nodes) {
      let member: GithubPrepareMemberOutput
      if (reply.author?.login) {
        member = await prepareMember(reply.author, ctx)
      } else if (reply.authorBot?.login) {
        member = prepareBotMember(reply.authorBot)
      } else if (reply.author === null && reply.authorBot === null) {
        member = prepareDeletedMember()
      } else {
        ctx.log.warn(
          'Discussion comment reply author is not found. This discussion comment reply will not be parsed.',
        )
        continue
      }

      // publish data
      await ctx.processData<GithubApiData>({
        type: GithubActivityType.DISCUSSION_COMMENT,
        subType: GithubActivitySubType.DISCUSSION_COMMENT_REPLY,
        data: reply,
        member,
        sourceParentId: commentId,
        repo: data.repo,
        isOld: true,
      })
    }
  }
}

const handler: ProcessStreamHandler = async (ctx) => {
  await timeout(1000)
  const streamIdentifier = ctx.stream.identifier
  if (streamIdentifier.startsWith(GithubStreamType.ROOT)) {
    await processRootStream(ctx)
  } else {
    const data = ctx.stream.data as GithubBasicStream
    const repo = getRepoByName(data.repo.name, ctx)

    if (repo === null) {
      await ctx.abortWithError(
        `Stream ${ctx.stream.identifier} can't be processed since repo ${data.repo.name} is not found!`,
        {
          repoName: data.repo.name,
          streamIdentifier: ctx.stream.identifier,
        },
      )
    }

    if (!repo.available) {
      await ctx.abortWithError(
        `Stream ${ctx.stream.identifier} can't be processed since repo ${data.repo.name} is not available!`,
        {
          repoName: data.repo.name,
          streamIdentifier: ctx.stream.identifier,
        },
      )
    }

    // if all checks are passed, we can start processing

    // Extract the stream type from the identifier
    const streamType = streamIdentifier.split(':')[0]

    switch (streamType) {
      case GithubStreamType.STARGAZERS:
        await processStargazersStream(ctx)
        break
      case GithubStreamType.FORKS:
        await processForksStream(ctx)
        break
      case GithubStreamType.PULLS:
        await processPullsStream(ctx)
        break
      case GithubStreamType.PULL_COMMENTS:
        await processPullCommentsStream(ctx)
        break
      case GithubStreamType.PULL_REVIEW_THREADS:
        await processPullReviewThreadsStream(ctx)
        break
      case GithubStreamType.PULL_REVIEW_THREAD_COMMENTS:
        await processPullReviewThreadCommentsStream(ctx)
        break
      case GithubStreamType.PULL_COMMITS:
        await processPullCommitsStream(ctx)
        break
      case GithubStreamType.ISSUES:
        await processIssuesStream(ctx)
        break
      case GithubStreamType.ISSUE_COMMENTS:
        await processIssueCommentsStream(ctx)
        break
      case GithubStreamType.DISCUSSIONS:
        await processDiscussionsStream(ctx)
        break
      case GithubStreamType.DISCUSSION_COMMENTS:
        await processDiscussionCommentsStream(ctx)
        break
      default:
        console.error(`No matching process function for streamType: ${streamType}`)
    }
  }
}

export default handler
