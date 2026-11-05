export type SystemSettingTypes = {
  memberDisplayAggsLastSyncedAt: { timestamp: string }
  organizationDisplayAggsLastSyncedAt: { timestamp: string }
  memberCountryInferenceLastSyncedAt: { timestamp: string }
  organizationCountryInferenceLastSyncedAt: { timestamp: string }
  resolveGitAuthorsCursor: { afterEmail: string | null }
}

export type SystemSettingKey = keyof SystemSettingTypes
