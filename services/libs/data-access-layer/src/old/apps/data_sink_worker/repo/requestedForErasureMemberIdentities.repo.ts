import { DbStore, RepositoryBase } from '@crowd/database'
import { Logger } from '@crowd/logging'
import { IMemberIdentity, MemberIdentityType } from '@crowd/types'

export default class RequestedForErasureMemberIdentitiesRepository extends RepositoryBase<RequestedForErasureMemberIdentitiesRepository> {
  constructor(store: DbStore, parentLog: Logger) {
    super(store, parentLog)
  }

  public async someIdentitiesWereErasedByUserRequest(
    identities: IMemberIdentity[],
  ): Promise<IMemberIdentity[]> {
    if (identities.length === 0) {
      return []
    }

    const toErase = []

    const results = await this.wereIdentitiesRequestedForErasure(identities)

    for (const [identity, wasRequested] of results.entries()) {
      if (wasRequested) {
        toErase.push(identity)
      }
    }

    return toErase
  }

  private async wereIdentitiesRequestedForErasure(
    identities: IMemberIdentity[],
  ): Promise<Map<IMemberIdentity, boolean>> {
    const results = new Map<IMemberIdentity, boolean>()

    const params: Record<string, unknown> = {}

    const identityConditions: string[] = []
    let index = 0
    for (const identity of identities) {
      if (!identity.value) {
        continue
      }

      const typeParam = `type_${index++}`
      const valueParam = `value_${index++}`

      const conditions: string[] = [`type = $(${typeParam})`, `lower(value) = $(${valueParam})`]

      params[typeParam] = identity.type
      params[valueParam] = identity.value.toLowerCase()

      if (identity.type !== MemberIdentityType.EMAIL) {
        const platformParam = `platform_${index++}`
        conditions.push(`platform = $(${platformParam})`)
        params[platformParam] = identity.platform
      }

      identityConditions.push(`(${conditions.join(' and ')})`)
    }

    const data = await this.db().any(
      `
      select id, type, value, platform from "requestedForErasureMemberIdentities"
      where ${identityConditions.join(' or ')}
      `,
      params,
    )

    for (const identity of identities) {
      if (!identity.value) {
        results.set(identity, false)
        continue
      }

      if (identity.type === MemberIdentityType.EMAIL) {
        // Email is platform-agnostic: the SQL query also omits the platform filter for EMAIL.
        // Using find (not singleOrDefault) because case-insensitive matching can return
        // multiple rows for the same email stored with different casing.
        const row =
          data.find(
            (r) =>
              r.type === identity.type && r.value.toLowerCase() === identity.value.toLowerCase(),
          ) ?? null

        results.set(identity, row !== null)
      } else {
        // The SQL query filters non-EMAIL identities by platform, so the in-memory filter
        // must also include platform. Without it, two identities sharing (type, value) but
        // on different platforms would both match. The previous singleOrDefault-based
        // implementation threw on multiple matches; find is safe and returns the first match.
        const row =
          data.find(
            (r) =>
              r.type === identity.type &&
              r.value.toLowerCase() === identity.value.toLowerCase() &&
              r.platform === identity.platform,
          ) ?? null

        results.set(identity, row !== null)
      }
    }

    return results
  }

  private async wasIdentityRequestedForErasure(
    identity: IMemberIdentity,
  ): Promise<{ id: string } | null> {
    if (!identity.value) {
      return null
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const params: any = {
      type: identity.type,
      value: identity.value.toLowerCase(),
    }
    const conditions: string[] = ['r.type = $(type)', 'lower(r.value) = $(value)']

    if (identity.type !== MemberIdentityType.EMAIL) {
      conditions.push('r.platform = $(platform)')
      params.platform = identity.platform
    }

    const query = `
      select r.id from "requestedForErasureMemberIdentities" r
      where
        ${conditions.join(' and ')}
      limit 1
    `

    return await this.db().oneOrNone(query, params)
  }
}
