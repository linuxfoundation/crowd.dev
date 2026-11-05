import { answerProjectCatalogQuestion, findProjectCatalogById } from '@crowd/data-access-layer'
import {
  IDbProjectCatalog,
  IDbProjectCatalogAnswer,
} from '@crowd/data-access-layer/src/project-catalog/types'
import {
  applyRequestStatus,
  buildBotAnsweredContent,
  decodeAnswerValue,
  describeAnsweredQuestion,
  findOrganizationSiblings,
  findQuestionOption,
  handOffReason,
  organizationReviewOf,
  refreshRequestStatusMessage,
} from '@crowd/project-onboarding'
import { getSlackBotConfig } from '@crowd/slack'
import { WorkflowIdReusePolicy } from '@crowd/temporal'

import { IServiceOptions } from '../IServiceOptions'
import type { OnboardingRequestTemporal } from './onboardingRequestBot'
import { postToResponseUrl, textMessage } from './onboardProjectCommand'
import { getBgQx } from './slackBackground'

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

export interface IAnswerRequestCommand {
  value: string
  responseUrl: string
  actorId: string
  temporal: OnboardingRequestTemporal | null
  log: IServiceOptions['log']
}

function postEphemeral(responseUrl: string, text: string, log: IServiceOptions['log']) {
  const ephemeral = { ...textMessage(text), response_type: 'ephemeral', replace_original: false }
  return postToResponseUrl(responseUrl, ephemeral, log)
}

export function postAnswerRequestFailure(responseUrl: string, log: IServiceOptions['log']) {
  return postEphemeral(
    responseUrl,
    ':warning: Your answer could not be recorded. Please try again in a minute.',
    log,
  )
}

function canAnswer(row: IDbProjectCatalog, actorId: string): boolean {
  return row.requestedBy === actorId || getSlackBotConfig().reviewerUserIds.includes(actorId)
}

async function startAnswerEvaluation(
  temporal: OnboardingRequestTemporal | null,
  catalogIds: string[],
  log: IServiceOptions['log'],
): Promise<void> {
  const [catalogId] = catalogIds
  if (!temporal) {
    log.warn({ catalogIds }, 'Temporal not configured, the answered rows wait for the schedule.')
    return
  }
  try {
    await temporal.workflow.start('evaluateProjects', {
      taskQueue: 'projects-evaluation',
      workflowId: `evaluate-answer-${catalogId}-${Date.now()}`,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      args: [{ catalogIds }],
    })
  } catch (err) {
    log.error({ catalogIds, err }, 'Starting the evaluation after the answer failed.')
  }
}

export async function runAnswerRequestCommand({
  value,
  responseUrl,
  actorId,
  temporal,
  log,
}: IAnswerRequestCommand): Promise<void> {
  const decoded = decodeAnswerValue(value)
  if (!decoded || !UUID_PATTERN.test(decoded.catalogId)) {
    log.warn({ value }, 'Answer click carried an invalid value.')
    await postEphemeral(responseUrl, ':no_entry: This question is no longer valid.', log)
    return
  }
  const { catalogId, optionId } = decoded

  const qx = await getBgQx()
  const row = await findProjectCatalogById(qx, catalogId)
  if (!row) {
    await postEphemeral(responseUrl, ':no_entry: This question is no longer valid.', log)
    return
  }
  if (!canAnswer(row, actorId)) {
    const who = row.requestedBy ? `<@${row.requestedBy}>` : 'the requester'
    await postEphemeral(responseUrl, `Only ${who} or the CDP team can answer this.`, log)
    return
  }
  const option = findQuestionOption(row.requestQuestion, optionId)
  if (row.action !== 'ask' || !row.requestQuestion || !option) {
    const answered = row.requestQuestion ? describeAnsweredQuestion(row.requestQuestion) : null
    await postEphemeral(responseUrl, answered ?? 'This question was already answered.', log)
    return
  }

  const messageTs = row.requestQuestion.messageTs
  const siblings = await findOrganizationSiblings(
    qx,
    row,
    (sibling) =>
      !!messageTs && sibling.action === 'ask' && sibling.requestQuestion?.messageTs === messageTs,
  )
  const answer: IDbProjectCatalogAnswer = {
    answer: option.id,
    answeredBy: actorId,
    hints: option.hints ?? null,
    action: option.handOff ? 'review' : 'evaluate',
    reviewReason: option.handOff ? handOffReason(row, row.requestQuestion) : null,
    proposedAction: option.handOff ? 'human_review' : null,
  }
  const { answered, catalogIds } = await qx.tx(async (tx) => {
    const lead = await answerProjectCatalogQuestion(tx, catalogId, answer)
    if (!lead) {
      return { answered: null, catalogIds: [] as string[] }
    }
    const ids = [catalogId]
    for (const sibling of siblings) {
      if (await answerProjectCatalogQuestion(tx, sibling.id, answer)) {
        ids.push(sibling.id)
      }
    }
    return { answered: lead, catalogIds: ids }
  })
  if (!answered) {
    await postEphemeral(responseUrl, 'This question was already answered.', log)
    return
  }
  const organization = organizationReviewOf(row, catalogIds.length - 1)

  log.info(
    { catalogId, optionId, actorId, siblings: catalogIds.length - 1 },
    'Requester answered the question.',
  )
  const answeredContent = buildBotAnsweredContent(
    row,
    row.requestQuestion,
    option,
    actorId,
    organization,
  )
  const replaced = {
    text: answeredContent.text,
    blocks: answeredContent.blocks ?? [],
    replace_original: true,
  }
  await postToResponseUrl(responseUrl, replaced, log)
  await refreshRequestStatusMessage(qx, answered)
  await applyRequestStatus(qx, answered, option.handOff ? 'withTeam' : 'checking')
  await startAnswerEvaluation(temporal, catalogIds, log)
}
