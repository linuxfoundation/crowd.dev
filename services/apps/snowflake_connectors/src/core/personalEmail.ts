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

const SHARED_PLACEHOLDER_EMAILS = [
  'email@email.com',
  'x@x.com',
  'na@na.com',
  'none@none.com',
  'null@null.com',
  'fake@fake.com',
  'abc@abc.com',
  'test@gmail.com',
]

const PLACEHOLDER_EMAIL_TEXT = ['NONE', 'NULL', 'UNDEFINED', 'NA', 'N/A', '-']

const PLACEHOLDER_EMAIL_DOMAINS = ['example.com', 'example.org', 'example.net', 'test.com']

const NO_REPLY_LOCAL_PARTS = ['noreply', 'no-reply', 'donotreply']

const sqlList = (values: Iterable<string>): string =>
  `(${[...values].map((value) => `'${value}'`).join(', ')})`

export function personalEmailCondition(column: string): string {
  const normalized = `LOWER(TRIM(${column}))`
  return `NULLIF(TRIM(${column}), '') IS NOT NULL
    AND UPPER(TRIM(${column})) NOT IN ${sqlList(PLACEHOLDER_EMAIL_TEXT)}
    AND ${normalized} NOT IN ${sqlList([...SHARED_MEETING_ACCOUNT_EMAILS, ...SHARED_PLACEHOLDER_EMAILS])}
    AND SPLIT_PART(${normalized}, '@', 2) NOT IN ${sqlList(PLACEHOLDER_EMAIL_DOMAINS)}
    AND SPLIT_PART(${normalized}, '@', 1) NOT IN ${sqlList(NO_REPLY_LOCAL_PARTS)}`
}

export function isSharedMeetingAccountEmail(email: string): boolean {
  return SHARED_MEETING_ACCOUNT_EMAILS.has(email.toLowerCase())
}

export function cleanPersonalEmail(raw: unknown): string | null {
  const email = cleanIdentityValue(raw)
  return email && !isSharedMeetingAccountEmail(email) ? email : null
}
