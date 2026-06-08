import commandLineArgs from 'command-line-args'

import { signalMemberUpdate } from '@crowd/common_services'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'

import { TEMPORAL_CONFIG } from '@/conf'

const log = getServiceLogger()

const options = [
  {
    name: 'memberIds',
    type: String,
    multiple: true,
  },
]

const parameters = commandLineArgs(options)

function parseMemberIds(value?: string[]): string[] {
  if (!value?.length) {
    return []
  }

  return [
    ...new Set(
      value
        .flatMap((item) => item.split(','))
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  ]
}

setImmediate(async () => {
  const memberIds = parseMemberIds(parameters.memberIds)

  if (memberIds.length === 0) {
    log.error('No memberIds provided')
    process.exit(1)
  }

  log.info({ memberCount: memberIds.length }, 'Triggering member updates')

  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  for (const memberId of memberIds) {
    await signalMemberUpdate(temporal, memberId)
  }

  log.info({ memberCount: memberIds.length }, 'Finished triggering member updates')
  process.exit(0)
})