/* eslint-disable @typescript-eslint/no-unused-vars */
export const keepPrimary = (primary: any, secondary: any): any => primary

const hasValue = (value: unknown): boolean => {
  if (Array.isArray(value)) {
    return value.length > 0
  }

  if (value && typeof value === 'object') {
    return Object.keys(value).length > 0
  }

  return Boolean(value)
}

export const keepPrimaryIfExists = (primary: any, secondary: any): any =>
  hasValue(primary) ? primary : secondary

export const mergeUniqueStringArrayItems = (primary: string[], secondary: string[]): string[] => {
  if (!primary || primary.length === 0) {
    return secondary
  }

  if (!secondary || secondary.length === 0) {
    return primary
  }

  return [...new Set([...primary, ...secondary])]
}
