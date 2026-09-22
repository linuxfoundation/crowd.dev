import { getServiceChildLogger } from '@crowd/logging'
import { RedisCache, RedisClient } from '@crowd/redis'

import { QueryExecutor } from '../queryExecutor'
import { IDbMemberAttributeSetting } from './types'

const log = getServiceChildLogger('db/members/attributeSettings')

let cache: RedisCache | undefined

export function getMemberAttributeSettingsCache(redis: RedisClient): RedisCache {
  if (!cache) {
    cache = new RedisCache('memberAttributes', redis, log)
  }

  return cache
}

export async function getPlatformPriorityArray(qx: QueryExecutor): Promise<string[] | undefined> {
  const results = await qx.selectOneOrNone(
    `select ("attributeSettings" -> 'priorities') as priorities from settings`,
  )

  return results?.priorities || undefined
}

export async function getMemberAttributeSettings(
  qx: QueryExecutor,
  redis: RedisClient,
): Promise<IDbMemberAttributeSetting[]> {
  const cache = getMemberAttributeSettingsCache(redis)
  const cached = await cache.get('default')

  if (!cached) {
    const results = await qx.select(
      `
    select id, type, "canDelete", show, label, name, options
    from "memberAttributeSettings"
    `,
    )

    const data = results.map((r) => {
      return {
        id: r.id,
        type: r.type,
        canDelete: r.canDelete,
        show: r.show,
        label: r.label,
        name: r.name,
        options: r.options,
      }
    })

    await cache.set('default', JSON.stringify(data), 30 * 60)

    return data
  }

  return JSON.parse(cached)
}
