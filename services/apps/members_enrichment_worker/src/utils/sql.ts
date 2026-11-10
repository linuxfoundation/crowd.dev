import { MemberEnrichmentSource } from '@crowd/types'

export function hasVerifiedIdentitySql(condition: string): string {
  return `EXISTS (
    SELECT 1 FROM "memberIdentities" mi
    WHERE mi."memberId" = "membersGlobalActivityCount"."memberId"
      AND mi."deletedAt" IS NULL
      AND mi.verified
      AND (${condition})
  )`
}

export function hasNewCachedInputsSql(
  source: MemberEnrichmentSource,
  inputSources: MemberEnrichmentSource[],
): string {
  return `EXISTS (
    SELECT 1 FROM "memberEnrichmentCache" input_cache
    WHERE input_cache."memberId" = "membersGlobalActivityCount"."memberId"
      AND input_cache.source IN (${inputSources.map((inputSource) => `'${inputSource}'`).join(', ')})
      AND input_cache.data IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM "memberEnrichmentCache" own_cache
        WHERE own_cache."memberId" = input_cache."memberId"
          AND own_cache.source = '${source}'
          AND own_cache.data IS NULL
          AND own_cache."updatedAt" >= input_cache."updatedAt"
      )
  )`
}
