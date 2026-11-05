import { describe, expect, it } from 'vitest'

import { cleanPersonalEmail, isSharedMeetingAccountEmail } from './personalEmail'

describe('isSharedMeetingAccountEmail', () => {
  it.each(['zoom@hyperledger.org', 'Zoom2@LFAI.foundation', 'meetings@dpdk.org'])(
    'flags the known shared meeting host account %s',
    (email) => {
      expect(isSharedMeetingAccountEmail(email)).toBe(true)
    },
  )

  it.each([
    'zoom3@hyperledger.org',
    'zoom@antonpalmqvist.com',
    'james.wang@zoom.us',
    'noreply@github.com',
    'mail@jkroepke.de',
  ])('does not flag %s, which is not on the known list', (email) => {
    expect(isSharedMeetingAccountEmail(email)).toBe(false)
  })
})

describe('cleanPersonalEmail', () => {
  it('trims and keeps a personal email', () => {
    expect(cleanPersonalEmail(' ada@lovelace.dev ')).toBe('ada@lovelace.dev')
  })

  it('drops a known shared meeting host account', () => {
    expect(cleanPersonalEmail(' zoom@openwallet.foundation ')).toBeNull()
  })

  it('drops a placeholder email value', () => {
    expect(cleanPersonalEmail('NONE')).toBeNull()
  })
})
