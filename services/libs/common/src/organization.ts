export function generateOrganizationNameVariants(name: string): string[] {
  const exact = name.trim().toLowerCase().replace(/\s+/g, ' ')
  if (!exact) {
    return []
  }

  const variants = new Set<string>([exact])
  const add = (value: string) => {
    const normalized = value.trim().toLowerCase().replace(/\s+/g, ' ')
    if (normalized) {
      variants.add(normalized)
    }
  }

  let withoutParens = exact
  if (exact.endsWith(')')) {
    const open = exact.lastIndexOf('(')
    if (open !== -1 && !exact.slice(open + 1, -1).includes(')')) {
      withoutParens = exact.slice(0, open).trimEnd()
    }
  }
  if (withoutParens !== exact && withoutParens.length >= 8) {
    add(withoutParens)
  }

  for (const value of [...variants]) {
    if (value.startsWith('the ') && value.slice(4).length >= 8) {
      add(value.slice(4))
    }
  }

  for (const value of [...variants]) {
    for (const suffix of ['project', 'foundation', 'initiative']) {
      const token = ` ${suffix}`
      if (value.endsWith(token)) {
        const base = value.slice(0, -token.length).trim()
        if (base.length >= 6) {
          add(base)
        }
      } else if (value.length >= 4 && !value.includes('(')) {
        add(`${value}${token}`)
      }
    }
  }

  for (const value of [...variants]) {
    if (value.includes('-')) {
      add(value.replace(/-/g, ' '))
    }
    if (value.includes(' ')) {
      add(value.replace(/ /g, '-'))
    }
    if (value.includes('.')) {
      add(value.replace(/\./g, ''))
    }
  }

  return [...variants]
}
