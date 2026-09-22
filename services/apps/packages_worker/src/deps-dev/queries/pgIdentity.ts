// The (namespace, name) split ingestPackages.ts writes into `packages` (see MERGE_SQL there).
// Any consumer that needs to join back onto that identity by name must derive it identically,
// so both sides share this one definition instead of drifting apart.
export function packageNameSplitSql(
  alias: string,
  nameCol: string,
): { namespace: string; name: string } {
  const ecosystem = `${alias}.ecosystem`
  const rawName = `${alias}.${nameCol}`
  return {
    namespace: `CASE
      WHEN ${ecosystem} = 'maven' THEN SPLIT_PART(${rawName}, ':', 1)
      WHEN ${rawName} LIKE '@%/%' THEN SPLIT_PART(${rawName}, '/', 1)
      ELSE NULL
    END`,
    name: `CASE
      WHEN ${ecosystem} = 'maven' THEN SPLIT_PART(${rawName}, ':', 2)
      WHEN ${rawName} LIKE '@%/%' THEN SPLIT_PART(${rawName}, '/', 2)
      ELSE ${rawName}
    END`,
  }
}
