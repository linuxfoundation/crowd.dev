interface OverrideBase {
  badgeTemplateId: string
  reviewedBy: string
  note: string
}

export type CredlyTemplateOverride =
  | (OverrideBase & { status: 'mapped'; projectSlug: string })
  | (OverrideBase & { status: 'excluded' })

const SHEET_3 = 'sheet 3 of the Credly template mapping review of 2026-11-10, decided 2026-11-12'

const reviewed = (badgeTemplateId: string, projectSlug: string): CredlyTemplateOverride => ({
  badgeTemplateId,
  status: 'mapped',
  projectSlug,
  reviewedBy: 'caitriona',
  note: SHEET_3,
})

// Overrides win over every rule, so a reviewer decision is not undone by the next resolver run.
export const credlyTemplateOverrides: readonly CredlyTemplateOverride[] = [
  reviewed('26a91b61-b8cc-4c54-a3a6-7ad7192d14d3', 'green-software'),
  reviewed('5a8aae06-003b-4547-96d4-e88a692a4e88', 'tlf'),
  reviewed('9f108577-e241-4338-b342-5a4649c742e2', 'tlf'),
  reviewed('ffb37093-26d7-4f47-9364-14af1ff799b2', 'tlf'),
  reviewed('612acf39-360a-4d2a-8522-8f44dc28856e', 'tlf'),
  reviewed('23622b5f-4b13-4d71-98f5-5774a3a9e3a2', 'tlf'),
  reviewed('7f7a0067-7fe7-4396-9acc-c2324f6c22e6', 'tlf'),
  reviewed('1224e06f-9940-47f8-8909-d7ac9923609d', 'tlf'),
  reviewed('6ad11b04-db90-44d2-9a73-563921478997', 'tlf'),
  reviewed('f179f812-69e6-4684-9f3d-9857f93ba1cf', 'tlf'),
  reviewed('c401c479-24b2-49d6-89be-3ef2b30c07af', 'tlf'),
  reviewed('bf90aaba-b71e-49d3-b113-2e62fa997a71', 'tlf'),
  reviewed('89a9984a-dd0e-4256-8848-8895986a1bdd', 'tlf'),
  reviewed('07c7c33a-3d50-456b-a66f-89267916618a', 'tlf'),
  reviewed('b1ae8720-d7f4-441c-afdc-359cd8463ed3', 'tlf'),
]
