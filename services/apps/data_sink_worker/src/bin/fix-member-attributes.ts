import isEqual from 'lodash.isequal'
import mergeWith from 'lodash.mergewith'

import { connQx, updateMember } from '@crowd/data-access-layer'
import {
  DbConnOrTx,
  DbStore,
  WRITE_DB_CONFIG,
  getDbConnection,
} from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { REDIS_CONFIG, getRedisClient } from '@crowd/redis'

import MemberAttributeService from '../service/memberAttribute.service'

/* eslint-disable @typescript-eslint/no-explicit-any */

const BATCH_SIZE = process.env.TEST_RUN ? 1 : 5000

const log = getServiceLogger()

async function getMemberIds(
  db: DbConnOrTx,
  lastId?: string,
): Promise<{ id: string; attributes: any; manuallyChangedFields: any }[]> {
  try {
    log.debug({ lastId }, 'Querying for members with attribute issues')

    const results = await db.any(
      `
      with relevant_members as (with member_with_attributes as (select id,
                                                                 "createdAt",
                                                                 jsonb_object_keys(attributes)               as attr_key,
                                                                 attributes -> jsonb_object_keys(attributes) as attr_value
                                                          from members
                                                          where "deletedAt" is null
                                                            and attributes is not null
                                                            and attributes != 'null'::jsonb
                                                            and attributes != '{}'::jsonb)
                          select distinct id
                          from member_with_attributes
                          where jsonb_typeof(attr_value) = 'object'
                            and coalesce(attr_value ->> 'default', '') = ''
                            and exists (select 1
                                        from jsonb_each_text(attr_value) as kv
                                        where kv.key != 'default'
                                          and coalesce(kv.value, '') != '')
                                          ${lastId ? `and id < '${lastId}'` : ''}
                                          order by id desc
                          limit ${BATCH_SIZE})
select m.id, m.attributes
from members m
         inner join
     relevant_members rm on rm.id = m.id;
      `,
      { lastId },
    )

    log.debug(
      {
        resultCount: results.length,
        lastId,
        firstId: results.length > 0 ? results[0].id : null,
        lastResultId: results.length > 0 ? results[results.length - 1].id : null,
      },
      'Query completed',
    )

    return results
  } catch (error) {
    log.error(
      {
        error: error.message,
        lastId,
        stack: error.stack,
      },
      'Failed to query member IDs',
    )
    throw error
  }
}

setImmediate(async () => {
  let dbClient: DbConnOrTx | undefined
  let redisClient: any | undefined

  try {
    log.info('Starting member attributes fix script')

    // Initialize connections
    log.info('Connecting to database...')
    dbClient = await getDbConnection(WRITE_DB_CONFIG())
    log.info('Database connection established')

    log.info('Connecting to Redis...')
    redisClient = await getRedisClient(REDIS_CONFIG())
    log.info('Redis connection established')

    const pgQx = connQx(dbClient)
    const mas = new MemberAttributeService(redisClient, new DbStore(log, dbClient), log)

    let totalProcessed = 0
    let totalUpdated = 0
    let batchNumber = 1

    log.info('Starting to process members with attribute issues')
    let membersToFix = await getMemberIds(dbClient)
    log.info({ count: membersToFix.length }, 'Found members to process in first batch')

    while (membersToFix.length > 0) {
      log.info({ batchNumber, batchSize: membersToFix.length }, 'Processing batch')
      let batchUpdated = 0

      for (const data of membersToFix) {
        try {
          if (data.attributes) {
            log.debug(
              { memberId: data.id, oldAttributes: data.attributes },
              'Processing member attributes',
            )

            // check if any has default empty but other are full
            let toProcess = false
            for (const attName of Object.keys(data.attributes)) {
              const defValue = data.attributes[attName].default

              if (defValue === undefined || defValue === null || String(defValue) === '') {
                log.debug(
                  {
                    memberId: data.id,
                    attribute: data.attributes[attName],
                    attName,
                    defValue: defValue ? String(defValue) : 'undefined',
                  },
                  'Attribute has default empty',
                )
                for (const platform of Object.keys(data.attributes[attName]).filter(
                  (p) => p !== 'default',
                )) {
                  const value = data.attributes[attName][platform]

                  if (value !== undefined && value !== null && String(value) !== '') {
                    log.debug(
                      { memberId: data.id, attName, platform, value },
                      'Found value for attribute',
                    )
                    toProcess = true
                    break
                  }
                }

                if (toProcess) {
                  break
                }
              }
            }

            if (toProcess) {
              const oldAttributes = JSON.parse(JSON.stringify(data.attributes)) // Deep copy
              data.attributes = await mas.setAttributesDefaultValues(data.attributes)

              let attributes: Record<string, unknown> | undefined
              const temp = mergeWith({}, oldAttributes, data.attributes)
              const manuallyChangedFields: string[] = data.manuallyChangedFields || []

              if (manuallyChangedFields.length > 0) {
                log.warn(
                  {
                    memberId: data.id,
                    manuallyChangedFieldsCount: manuallyChangedFields.length,
                  },
                  'Member has manually changed fields',
                )

                const prefix = 'attributes.'
                const manuallyChangedAttributes = [
                  ...new Set(
                    manuallyChangedFields
                      .filter((f) => f.startsWith(prefix))
                      .map((f) => f.slice(prefix.length)),
                  ),
                ]

                log.warn(
                  {
                    memberId: data.id,
                    manuallyChangedAttributes,
                  },
                  'Preserving manually changed attributes',
                )

                // Preserve manually changed attributes
                for (const key of manuallyChangedAttributes) {
                  if (oldAttributes?.[key] !== undefined) {
                    temp[key] = oldAttributes[key] // Fixed: removed .attributes
                  }
                }
              }

              if (!isEqual(temp, oldAttributes)) {
                attributes = temp
                log.info({ memberId: data.id }, 'Attributes changed, will update')
              } else {
                log.debug(
                  { memberId: data.id, newAttributes: temp, oldAttributes },
                  'No changes needed for attributes',
                )
              }

              if (attributes) {
                log.info({ memberId: data.id }, 'Updating member attributes')

                if (!process.env.TEST_RUN) {
                  await updateMember(pgQx, data.id, { attributes } as any)
                }

                batchUpdated++
                totalUpdated++
                log.debug({ memberId: data.id }, 'Member attributes updated successfully')
              }
            } else {
              log.debug(
                { memberId: data.id, attributes: data.attributes },
                'No changes needed for attributes',
              )
            }
          } else {
            log.debug({ memberId: data.id }, 'Member has no attributes to process')
          }

          totalProcessed++
        } catch (error) {
          log.error(
            {
              error: error.message,
              memberId: data.id,
              stack: error.stack,
            },
            'Failed to process member',
          )
          // Continue processing other members
        }
      }

      log.info(
        {
          batchNumber,
          batchProcessed: membersToFix.length,
          batchUpdated,
          totalProcessed,
          totalUpdated,
        },
        'Completed batch processing',
      )

      // Get next batch
      const lastId = membersToFix[membersToFix.length - 1].id
      log.debug({ lastId }, 'Fetching next batch starting from last ID')

      if (process.env.TEST_RUN) {
        break
      }

      membersToFix = await getMemberIds(dbClient, lastId)
      log.info({ count: membersToFix.length }, 'Found members for next batch')

      batchNumber++
    }

    log.info(
      {
        totalProcessed,
        totalUpdated,
        totalBatches: batchNumber - 1,
      },
      'Member attributes fix completed successfully',
    )
  } catch (error) {
    log.error(
      {
        error: error.message,
        stack: error.stack,
      },
      'Fatal error in member attributes fix script',
    )
    process.exit(1)
  } finally {
    log.info('Script execution completed')
    process.exit(0)
  }
})
