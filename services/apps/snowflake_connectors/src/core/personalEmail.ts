import { cleanIdentityValue } from './transformerBase'

export const SHARED_MEETING_ACCOUNT_EMAILS = new Set([
  'meeting1@seidengroup.com',
  'meetings@dpdk.org',
  'zoom-admin@nephio.org',
  'zoom0@hyperledger.org',
  'zoom2@hyperledger.org',
  'zoom2@lfai.foundation',
  'zoom@hyperledger.org',
  'zoom@longnowboston.org',
  'zoom@openwallet.foundation',
])

export function isSharedMeetingAccountEmail(email: string): boolean {
  return SHARED_MEETING_ACCOUNT_EMAILS.has(email.toLowerCase())
}

export function cleanPersonalEmail(raw: unknown): string | null {
  const email = cleanIdentityValue(raw)
  return email && !isSharedMeetingAccountEmail(email) ? email : null
}
