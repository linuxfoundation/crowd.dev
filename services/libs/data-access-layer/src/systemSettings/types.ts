export type SystemSettingTypes = {
  memberDisplayAggsLastSyncedAt: { timestamp: string }
  organizationDisplayAggsLastSyncedAt: { timestamp: string }
  memberCountryInferenceLastSyncedAt: { timestamp: string }
  organizationCountryInferenceLastSyncedAt: { timestamp: string }
}

export type SystemSettingKey = keyof SystemSettingTypes
