import { Message, Section, SlackMessageDto } from 'slack-block-builder'

import { getErrorMessage } from '@crowd/common'
import {
  IRequestClassification,
  buildRequestClassificationAlert,
  buildRequestClassificationAlertTitle,
  classifyOnboardingRequest,
} from '@crowd/project-onboarding'
import { withRequestClassifierDeps } from '@crowd/project-onboarding/src/requestClassifierDeps'
import { getSlackPermalink, postSlackMessage } from '@crowd/slack'

import { IServiceOptions } from '../IServiceOptions'
import { getBgQx } from './slackBackground'

const MENTION_PATTERN = /<@[A-Z0-9]+>/g
const LINK_WITH_LABEL_PATTERN = /<(https?:\/\/[^|>\s]+)\|[^>]*>/g
const LINK_PATTERN = /<(https?:\/\/[^>\s]+)>/g

const MAX_SECTION_TEXT = 2900

const HELP_TEXT =
  'Tell me about the project you want to onboard: its name, whether it is a Linux Foundation project and the GitHub repositories.'

function truncateSectionText(text: string): string {
  return text.length > MAX_SECTION_TEXT ? `${text.slice(0, MAX_SECTION_TEXT - 1)}…` : text
}

export function toRequestText(slackText: string): string {
  return slackText
    .replace(MENTION_PATTERN, '')
    .replace(LINK_WITH_LABEL_PATTERN, '$1')
    .replace(LINK_PATTERN, '$1')
    .trim()
}

export function buildClassificationReply(
  classification: IRequestClassification,
  requestUrl: string,
): SlackMessageDto {
  const alert = {
    sourceUrl: requestUrl,
    repoUrls: classification.trace.parsed?.githubRepoUrls ?? [],
    resolution: classification.resolution,
    dryRun: true,
  }
  const sections = buildRequestClassificationAlert(alert)

  return Message()
    .blocks(
      Section({ text: `*${buildRequestClassificationAlertTitle(alert)}*` }),
      ...sections.map(({ title, text }) =>
        Section({ text: truncateSectionText(title ? `*${title}*\n${text}` : text) }),
      ),
      Section({ text: `_Step reached: ${classification.node}_` }),
    )
    .buildToObject()
}

export async function runRequestClassificationBot({
  text,
  channelId,
  messageTs,
  threadTs,
  options,
}: {
  text: string
  channelId: string
  messageTs: string
  threadTs: string
  options: Pick<IServiceOptions, 'log'>
}): Promise<void> {
  const { log } = options
  const reply = async (message: SlackMessageDto | { text: string }) => {
    const result = await postSlackMessage({ channel: channelId, thread_ts: threadTs, ...message })
    if (!result.ok) {
      log.warn({ channelId, threadTs, error: result.error }, 'Slack bot reply was not delivered.')
    }
  }

  const requestText = toRequestText(text)
  if (!requestText) {
    await reply({ text: HELP_TEXT })
    return
  }

  try {
    const qx = await getBgQx()
    const classification = await withRequestClassifierDeps(qx, (deps) =>
      classifyOnboardingRequest(requestText, deps),
    )
    const requestUrl = (await getSlackPermalink(channelId, messageTs)) ?? ''

    log.info(
      { channelId, threadTs, node: classification.node, kind: classification.resolution.kind },
      'Onboarding request classified from Slack.',
    )
    await reply(buildClassificationReply(classification, requestUrl))
  } catch (err) {
    log.error({ error: getErrorMessage(err), channelId, threadTs }, 'Slack request failed.')
    await reply({ text: ':no_entry: I could not process this request, please try again later.' })
  }
}
