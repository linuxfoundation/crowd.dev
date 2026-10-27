export interface CourseCode {
  code: string
  base: string
}

// A code counts only when the name presents it as one: "LFS158: …", "TSCB - …" or "… (LFS248)".
// The first word of a name is never a code: "Exam Developer" and "LF Live" are words.
const LEADING_CODE = /^([A-Z][A-Z0-9]{1,9}(?:-[A-Z0-9]+)?)\s*(?::|\s-\s)/
const TRAILING_CODE = /\(([A-Z][A-Z0-9]{1,9})\)\s*$/

export function normalizeSku(sku: string): string {
  return sku.trim().toUpperCase()
}

export function skuBase(sku: string): string {
  return normalizeSku(sku).split(/[-_]/)[0]
}

export function extractCourseCode(templateName: string): CourseCode | null {
  const name = templateName.trim()
  const match = LEADING_CODE.exec(name) ?? TRAILING_CODE.exec(name)
  if (!match) return null
  const code = match[1]
  return { code, base: skuBase(code) }
}
