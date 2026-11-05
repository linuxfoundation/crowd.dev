import {
  IDbProjectCatalog,
  IRequestQuestion,
  IRequestQuestionOption,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { ISlackMessageContent, escapeSlackText, mrkdwnSections } from '@crowd/slack'

import { ANSWER_REQUEST_ACTION_ID, MAX_BUTTON_TEXT_LENGTH, STATUS, repoLabel } from './botCopy'
import { IOrganizationReview, organizationSubject } from './organizationGroup'

type QuestionRow = Pick<IDbProjectCatalog, 'id' | 'repoUrl' | 'requestQuestion'>
type KnownBlock = NonNullable<ISlackMessageContent['blocks']>[number]

const BUTTONS_PER_BLOCK = 5
export const ANSWER_VALUE_SEPARATOR = '|'

function buttonText(text: string): string {
  return text.length > MAX_BUTTON_TEXT_LENGTH
    ? `${text.slice(0, MAX_BUTTON_TEXT_LENGTH - 1)}…`
    : text
}

export function answerActionId(index: number): string {
  return `${ANSWER_REQUEST_ACTION_ID}:${index}`
}

export function encodeAnswerValue(catalogId: string, optionId: string): string {
  return `${catalogId}${ANSWER_VALUE_SEPARATOR}${optionId}`
}

export function decodeAnswerValue(value: string): { catalogId: string; optionId: string } | null {
  const separator = value.indexOf(ANSWER_VALUE_SEPARATOR)
  if (separator <= 0 || separator === value.length - 1) {
    return null
  }
  return { catalogId: value.slice(0, separator), optionId: value.slice(separator + 1) }
}

function optionButtons(catalogId: string, options: IRequestQuestionOption[]): KnownBlock[] {
  const blocks: KnownBlock[] = []
  for (let start = 0; start < options.length; start += BUTTONS_PER_BLOCK) {
    blocks.push({
      type: 'actions',
      elements: options.slice(start, start + BUTTONS_PER_BLOCK).map((option, offset) => ({
        type: 'button',
        action_id: answerActionId(start + offset),
        value: encodeAnswerValue(catalogId, option.id),
        text: { type: 'plain_text', text: buttonText(option.label) },
        ...(option.suggested ? { style: 'primary' as const } : {}),
      })),
    })
  }
  return blocks
}

function questionSubject(
  row: Pick<IDbProjectCatalog, 'repoUrl'>,
  organization: IOrganizationReview | null,
): string {
  return organization ? organizationSubject(organization) : repoLabel(row.repoUrl)
}

export function questionText(
  row: Pick<IDbProjectCatalog, 'repoUrl'>,
  question: IRequestQuestion,
  organization: IOrganizationReview | null = null,
) {
  return `${STATUS.needsInput} ${questionSubject(row, organization)}: ${escapeSlackText(question.text)}`
}

export function buildBotQuestionContent(
  row: QuestionRow,
  organization: IOrganizationReview | null = null,
): ISlackMessageContent | null {
  const question = row.requestQuestion
  if (!question) {
    return null
  }
  const text = questionText(row, question, organization)
  return { text, blocks: [...mrkdwnSections(text), ...optionButtons(row.id, question.options)] }
}

export function findQuestionOption(
  question: IRequestQuestion | null,
  optionId: string,
): IRequestQuestionOption | null {
  return question?.options.find((option) => option.id === optionId) ?? null
}

export function buildBotAnsweredContent(
  row: Pick<IDbProjectCatalog, 'repoUrl'>,
  question: IRequestQuestion,
  option: IRequestQuestionOption,
  answeredBy: string,
  organization: IOrganizationReview | null = null,
): ISlackMessageContent {
  const next = option.handOff
    ? "I've handed it to the CDP team; they'll decide here."
    : "Checking again with that, I'll update the status above."
  const text = `${questionText(row, question, organization)}\n<@${answeredBy}> answered: *${escapeSlackText(option.label)}*. ${next}`
  return { text, blocks: mrkdwnSections(text) }
}

function suggestedProjectName(question: IRequestQuestion): string | null {
  return (
    question.options.find((option) => option.hints?.pccProject)?.hints?.pccProject?.name ?? null
  )
}

export function handOffReason(
  row: Pick<IDbProjectCatalog, 'projectName'>,
  question: IRequestQuestion,
): string {
  const name = row.projectName
  const suggested = suggestedProjectName(question)
  switch (question.kind) {
    case 'did_you_mean':
      return name && suggested
        ? `the requester said ${name} isn't ${suggested}`
        : "the requester didn't recognise the project I suggested"
    case 'pick_project':
      return `the requester couldn't tell which ${name ? `${name} ` : ''}project this repo belongs to`
    case 'lf_conflict':
      return suggested
        ? `the requester isn't sure whether ${name ?? 'this repo'} is the LF project ${suggested}`
        : `the requester isn't sure whether ${name ?? 'this repo'} is an LF project`
    case 'lf_confirm':
      return `the requester isn't sure whether ${name ?? 'this project'} is an LF project`
  }
}

export function describeAnsweredQuestion(question: IRequestQuestion): string | null {
  if (!question.answeredBy || !question.answer) {
    return null
  }
  const option = findQuestionOption(question, question.answer)
  return `<@${question.answeredBy}> already answered this: ${option?.label ?? question.answer}.`
}
