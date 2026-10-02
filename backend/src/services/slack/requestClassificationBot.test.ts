import { describe, expect, it } from 'vitest'

import { IRequestClassification } from '@crowd/project-onboarding'

import { buildClassificationReply, toRequestText } from './requestClassificationBot'

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
})
