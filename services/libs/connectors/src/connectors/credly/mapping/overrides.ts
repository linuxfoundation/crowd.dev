interface OverrideBase {
  badgeTemplateId: string
  reviewedBy: string
  note: string
}

export type CredlyTemplateOverride =
  | (OverrideBase & { status: 'mapped'; projectSlug: string })
  | (OverrideBase & { status: 'excluded' })

// Overrides win over every rule, so a reviewer decision is not undone by the next resolver run.
export const credlyTemplateOverrides: readonly CredlyTemplateOverride[] = []
