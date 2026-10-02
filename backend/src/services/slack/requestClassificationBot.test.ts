import { describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/slack', () => ({ getSlackPermalink: vi.fn(), postSlackMessage: vi.fn() }))
vi.mock('@crowd/project-onboarding/src/requestClassifierDeps', () => ({
  withRequestClassifierDeps: vi.fn(),
}))
vi.mock('./slackBackground', () => ({ getBgQx: vi.fn() }))

import { IRequestClassification } from '@crowd/project-onboarding'

import {
  buildClassificationReply,
  hideFailureDetails,
  toRequestText,
} from './requestClassificationBot'

describe('toRequestText', () => {
  it('removes the bot mention and keeps the rest of the message', () => {
    expect(toRequestText('<@U0BOT> onboard Acme\nnot LF')).toBe('onboard Acme\nnot LF')
  })

  it('unwraps Slack links so the parser sees plain URLs', () => {
    expect(
      toRequestText(
        '<@U0BOT> <https://github.com/acme/one> and <https://github.com/acme/two|acme/two>',
      ),
    ).toBe('https://github.com/acme/one and https://github.com/acme/two')
  })

  it('decodes the HTML entities Slack applies to the message text', () => {
    expect(toRequestText('<@U0BOT> onboard R&amp;D &lt;internal&gt;')).toBe(
      'onboard R&D <internal>',
    )
  })

  it('returns an empty string when only the mention is left', () => {
    expect(toRequestText('<@U0BOT>  ')).toBe('')
  })
})

describe('buildClassificationReply', () => {
  const classification: IRequestClassification = {
    resolution: { kind: 'lf_not_in_pcc', projectName: 'Acme' },
    node: 'lf_not_in_pcc_flag_human',
    trace: {
      parsed: null,
      pccLookup: null,
      cdpLookup: null,
      failure: null,
    },
  }

  it('names the step reached and states that nothing was done', () => {
    const { blocks } = buildClassificationReply(classification, 'https://slack.test/thread')
    const text = JSON.stringify(blocks)

    expect(text).toContain('lf_not_in_pcc_flag_human')
    expect(text).toContain('[DRY RUN]')
    expect(text).toContain('https://slack.test/thread')
  })

  it('keeps every section under the Slack limit when the request lists many repositories', () => {
    const repoUrls = Array.from({ length: 300 }, (_, i) => `https://github.com/acme/repo-${i}`)
    const { blocks } = buildClassificationReply(
      {
        ...classification,
        trace: { ...classification.trace, parsed: { githubRepoUrls: repoUrls } as any },
      },
      'https://slack.test/thread',
    )

    const longest = Math.max(...blocks.map((block: any) => block.text?.text.length ?? 0))
    expect(longest).toBeLessThanOrEqual(3000)
  })

  it('adds a plain text fallback for clients that do not render blocks', () => {
    const message = buildClassificationReply(classification, 'https://slack.test/thread')

    expect(message.text).toContain('lf_not_in_pcc_flag_human')
  })

  it('does not expose dependency errors or model output in the reply', () => {
    const failed: IRequestClassification = {
      resolution: {
        kind: 'ambiguous',
        reason: 'Classification failed: SQL compilation error at line 11',
        candidates: [],
      },
      node: 'ambiguous_human_review',
      trace: {
        ...classification.trace,
        failure: { stage: 'resolve', reason: 'SQL compilation error at line 11' },
      },
    }

    const text = JSON.stringify(
      buildClassificationReply(hideFailureDetails(failed), 'https://slack.test/thread'),
    )

    expect(text).not.toContain('SQL compilation error')
    expect(text).toContain('could not be classified automatically')
  })
})
