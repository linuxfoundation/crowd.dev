import { describe, expect, it } from 'vitest'

import {
  buildBotAnsweredContent,
  buildBotQuestionContent,
  decodeAnswerValue,
  describeAnsweredQuestion,
  encodeAnswerValue,
  findQuestionOption,
  handOffReason,
} from './requestQuestion'

const question = {
  kind: 'did_you_mean' as const,
  text: 'I couldn\'t find a project called "Acme <Labs>". Did you mean Acme?',
  options: [
    {
      id: 'yes',
      label: 'Yes, Acme',
      hints: { pccProject: { projectId: 'p', name: 'Acme', slug: 'a', isLeaf: true } },
      suggested: true,
    },
    { id: 'no', label: 'No, ask the CDP team', handOff: true },
  ],
}
const pickQuestion = {
  kind: 'pick_project' as const,
  text: 'There are 2 Acme projects in LFX Insights. Which one should this repo go in?',
  options: [
    { id: 'seg-1', label: 'Acme · 28 repos', hints: { segmentId: 'seg-1' } },
    { id: 'seg-2', label: 'Acme · no repos yet', hints: { segmentId: 'seg-2' } },
    { id: 'not_sure', label: 'Not sure, ask the CDP team', handOff: true },
  ],
}
const conflictQuestion = {
  kind: 'lf_conflict' as const,
  text: "You said Acme isn't an LF project, but I found an LF project called Acme Labs. Which is right?",
  options: [
    {
      id: 'use_lf',
      label: 'Use the LF project Acme Labs',
      hints: { pccProject: { projectId: 'p', name: 'Acme Labs', slug: 'a', isLeaf: true } },
    },
    { id: 'not_lf', label: "It's not LF, treat it as new", hints: { treatAsNew: true } },
    { id: 'not_sure', label: 'Not sure, ask the CDP team', handOff: true },
  ],
}
const confirmQuestion = {
  kind: 'lf_confirm' as const,
  text: "I couldn't find Acme in the foundation project list. Is it a Linux Foundation project?",
  options: [
    { id: 'lf', label: "Yes, it's an LF project", hints: { declaredLf: true } },
    { id: 'not_lf', label: "No, it's not LF", hints: { treatAsNew: true } },
    { id: 'not_sure', label: 'Not sure, ask the CDP team', handOff: true },
  ],
}
const row = {
  id: '8f1c2a44-5b7e-4d2f-9c1a-3e6f7a8b9c0d',
  repoUrl: 'https://github.com/acme/widget',
  requestQuestion: question,
}

describe('buildBotQuestionContent', () => {
  it('posts the question with one button per option, the suggested one primary', () => {
    const content = buildBotQuestionContent(row)

    expect(content?.text).toBe(
      '❓ *<https://github.com/acme/widget|acme/widget>*: I couldn\'t find a project called "Acme &lt;Labs&gt;". Did you mean Acme?',
    )
    expect(content?.blocks).toMatchObject([
      { type: 'section' },
      {
        type: 'actions',
        elements: [
          {
            action_id: 'answer_request:0',
            value: `${row.id}|yes`,
            text: { text: 'Yes, Acme' },
            style: 'primary',
          },
          {
            action_id: 'answer_request:1',
            value: `${row.id}|no`,
            text: { text: 'No, ask the CDP team' },
          },
        ],
      },
    ])
    expect(buildBotQuestionContent({ ...row, requestQuestion: null })).toBeNull()
  })

  it('asks once for a whole organization', () => {
    const content = buildBotQuestionContent(row, { owner: 'acme', repositories: 5 })

    expect(content?.text).toBe(
      '❓ *github.com/acme* (5 repositories): I couldn\'t find a project called "Acme &lt;Labs&gt;". Did you mean Acme?',
    )
    expect(
      buildBotAnsweredContent(row, question, question.options[0], 'U1', {
        owner: 'acme',
        repositories: 5,
      }).text,
    ).toContain('❓ *github.com/acme* (5 repositories):')
  })

  it('leaves every button plain when the bot has no suggestion', () => {
    const content = buildBotQuestionContent({ ...row, requestQuestion: pickQuestion })

    const buttons = content?.blocks?.[1] as { elements: Array<{ style?: string }> }
    expect(buttons.elements).toHaveLength(3)
    expect(buttons.elements.every((button) => button.style === undefined)).toBe(true)
  })

  it('replaces the question with the answer and what happens next, buttons gone', () => {
    const answered = buildBotAnsweredContent(row, question, question.options[0], 'U1')
    expect(answered.text).toContain(
      "<@U1> answered: *Yes, Acme*. Checking again with that, I'll update the status above.",
    )
    expect(answered.blocks).toMatchObject([{ type: 'section' }])
    expect(buildBotAnsweredContent(row, question, question.options[1], 'U1').text).toContain(
      "I've handed it to the CDP team; they'll decide here.",
    )
  })
})

describe('answer values', () => {
  it('round-trips the catalog id and the option id', () => {
    expect(decodeAnswerValue(encodeAnswerValue(row.id, 'seg-1'))).toEqual({
      catalogId: row.id,
      optionId: 'seg-1',
    })
    expect(decodeAnswerValue('nonsense')).toBeNull()
    expect(decodeAnswerValue('abc|')).toBeNull()
  })

  it('describes a hand-off in a few words per question kind', () => {
    const named = { projectName: 'Acme Widgets' }
    expect(handOffReason(named, question)).toBe("the requester said Acme Widgets isn't Acme")
    expect(handOffReason({ projectName: null }, question)).toBe(
      "the requester didn't recognise the project I suggested",
    )
    expect(handOffReason(named, pickQuestion)).toBe(
      "the requester couldn't tell which Acme Widgets project this repo belongs to",
    )
    expect(handOffReason({ projectName: null }, pickQuestion)).toBe(
      "the requester couldn't tell which project this repo belongs to",
    )
    expect(handOffReason(named, conflictQuestion)).toBe(
      "the requester isn't sure whether Acme Widgets is the LF project Acme Labs",
    )
    expect(handOffReason(named, confirmQuestion)).toBe(
      "the requester isn't sure whether Acme Widgets is an LF project",
    )
  })

  it('finds options and describes an answered question', () => {
    expect(findQuestionOption(question, 'no')?.handOff).toBe(true)
    expect(findQuestionOption(question, 'maybe')).toBeNull()
    expect(describeAnsweredQuestion(question)).toBeNull()
    expect(describeAnsweredQuestion({ ...question, answeredBy: 'U1', answer: 'yes' })).toBe(
      '<@U1> already answered this: Yes, Acme.',
    )
  })
})
