import commandLineArgs from 'command-line-args'

import { DB_CONFIG, TEMPORAL_CONFIG } from '@/conf'
import { signalMemberUpdate } from '@crowd/common_services'
import { pgpQx } from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'
import { getTemporalClient } from '@crowd/temporal'
import { TemporalWorkflowId } from '@crowd/types'

const log = getServiceLogger()

const INDIVIDUAL_NO_ACCOUNT_ORGANIZATION_ID = '5264d0aa-93d5-4899-a22b-c00085ee41a5'

// every row the placeholder employer data fix touched carries its transaction timestamp
const DATA_FIX_TIMESTAMP = '2026-11-02 13:44:29.795236+00'

const DATA_FIX_ORGANIZATION_IDS = [
  '7d36dea6-c006-4f0a-9f1b-1760a760ae26',
  'e1ebc9e0-abe2-4839-b24b-aace243fe83c',
  '3776c953-6b70-4fb1-9b60-6f08845ccd96',
  '6894dce0-9e10-407a-aeb3-7975190ca09d',
  '0e6f1d26-457a-4dd1-971f-0cb8cd3e7466',
  'f1f2a216-8abe-4cde-8966-83764da73eab',
  '0d2493e0-8ff0-11f1-a142-3d165233a7ed',
  '26c1fc70-6825-11ee-bf47-1b8958bf42fa',
  'a11ad732-0d5b-4fda-a126-5e2eac0cbd07',
  'a9128630-09ff-11f1-b48c-fbe284aba2e4',
  'c71981d0-2848-11f1-acf6-0f57a5a155bb',
  'd952df95-7e1f-4662-a513-b210f8d6eeef',
  'f49d146d-9c8e-4c0e-9fcd-ea30e987c4bf',
  'a1cc8908-8f39-46b7-8809-1ba34fe3b5c7',
  '1e75e470-afb8-11ee-b3dd-2ff381180399',
  'bca388f0-4d28-11f0-bb0f-776f71d1f857',
  'ae1b6b1d-2752-4b51-b2d8-52fbbc017a7a',
  '6d0664c0-4daa-11f0-bb0f-776f71d1f857',
  '369a775a-e767-4b5a-88d5-b5097a804282',
  'fbff9900-335a-11ef-b49d-533ea14733fe',
  'fa1aa9d5-3cd7-4b7e-b662-612b0daa6253',
  'f2e5a20b-8213-4b4a-a595-f18f6202bb4c',
  '6dd9a370-28d9-11f1-b0a3-8ba1bee5c11a',
  'bb54e7fa-f582-4228-8b06-b845b66dab6b',
  'e4a3fff0-5eec-11ee-9375-ed1a83bac451',
  '0db66bd0-6852-11ee-9a5b-1f0695c38fcf',
  '09128160-3d80-11ee-9df7-318f14cb34ae',
  '31bee050-2e45-11f1-8489-7d4517c802f5',
  '562b3520-6e4f-11f1-9654-dd9da97a7d26',
  '8e61a7d0-6849-11ee-9fcf-b3ef1798ea27',
  'c8329a6c-5c2e-4a45-97dd-7d9131308566',
  '0225995f-4eff-4d2a-bd58-f8ab6de322a8',
  '29b501fa-a681-4fc8-b059-1405dc713e2b',
]

const PARALLEL_MEMBERS = 50

const options = [
  {
    name: 'startAfter',
    alias: 's',
    typeLabel: '{underline memberId}',
    type: String,
    description: 'Resume after this member ID (the last one logged as processed).',
  },
]

const parameters = commandLineArgs(options)

setImmediate(async () => {
  const startAfter: string | null = parameters.startAfter ?? null

  const db = await getDbConnection({
    host: DB_CONFIG.readHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })
  const qx = pgpQx(db)
  const temporal = await getTemporalClient(TEMPORAL_CONFIG)

  log.info({ startAfter }, 'Running script with the following parameters!')

  // loaded once because each query scans every work experience of Individual - No Account
  const rows: { memberId: string }[] = await qx.select(
    `
    select "memberId" from (
      select "memberId" from "memberOrganizations"
      where "organizationId" = $(individualNoAccountOrganizationId)
        and "updatedAt" = $(dataFixTimestamp)
      union
      select "memberId" from "memberOrganizations"
      where "organizationId" = any($(dataFixOrganizationIds)::uuid[])
        and "deletedAt" = $(dataFixTimestamp)
    ) affected
    where $(startAfter) is null or "memberId" > $(startAfter)::uuid
    order by "memberId"
    `,
    {
      individualNoAccountOrganizationId: INDIVIDUAL_NO_ACCOUNT_ORGANIZATION_ID,
      dataFixTimestamp: DATA_FIX_TIMESTAMP,
      dataFixOrganizationIds: DATA_FIX_ORGANIZATION_IDS,
      startAfter,
    },
  )
  const memberIds = rows.map((row) => row.memberId)

  log.info(`Found ${memberIds.length} members to process`)

  let processed = 0
  const failedMemberIds: string[] = []

  for (const chunk of chunkArray(memberIds, PARALLEL_MEMBERS)) {
    await Promise.all(
      chunk.map(async (memberId) => {
        try {
          await signalMemberUpdate(temporal, memberId, { syncToOpensearch: true })
          await temporal.workflow.result(`${TemporalWorkflowId.MEMBER_UPDATE}/${memberId}`)
        } catch (err) {
          failedMemberIds.push(memberId)
          log.error({ memberId, err }, 'Failed to recalculate affiliations for member!')
        }
      }),
    )

    processed += chunk.length
    log.info(
      {
        processed,
        failed: failedMemberIds.length,
        total: memberIds.length,
        lastMemberId: chunk[chunk.length - 1],
      },
      'Processed chunk!',
    )
  }

  log.info({ processed, failedMemberIds }, 'Script completed!')
  process.exit(failedMemberIds.length > 0 ? 1 : 0)
})
