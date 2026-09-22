export type WellKnownFileType =
  | 'security'
  | 'contributing'
  | 'governance'
  | 'maintainers'
  | 'code_of_conduct'
  | 'readme'

export type WellKnownDirectory = 'root' | '.github' | 'docs'

export interface TreeEntryNode {
  name: string
  type: string
  oid: string
}

export interface WellKnownFileEntry {
  fileType: WellKnownFileType
  directory: WellKnownDirectory
  path: string
  blobOid: string
}

export interface RepoTrees {
  root: TreeEntryNode[] | null
  github: TreeEntryNode[] | null
  docs: TreeEntryNode[] | null
}

const STEM_TO_TYPE: Record<string, WellKnownFileType> = {
  README: 'readme',
  SECURITY: 'security',
  CONTRIBUTING: 'contributing',
  GOVERNANCE: 'governance',
  MAINTAINERS: 'maintainers',
  CODEOWNERS: 'maintainers',
  CODE_OF_CONDUCT: 'code_of_conduct',
}

function stemOf(filename: string): string {
  return filename
    .replace(/\.[^.]+$/, '')
    .toUpperCase()
    .replace(/-/g, '_')
}

const DIRECTORIES: Array<{ key: keyof RepoTrees; directory: WellKnownDirectory; prefix: string }> =
  [
    { key: 'root', directory: 'root', prefix: '' },
    { key: 'github', directory: '.github', prefix: '.github/' },
    { key: 'docs', directory: 'docs', prefix: 'docs/' },
  ]

export function classifyWellKnownFiles(trees: RepoTrees): WellKnownFileEntry[] {
  const result: WellKnownFileEntry[] = []
  for (const { key, directory, prefix } of DIRECTORIES) {
    for (const entry of trees[key] ?? []) {
      if (entry.type !== 'blob') continue
      const fileType = STEM_TO_TYPE[stemOf(entry.name)]
      if (!fileType) continue
      result.push({ fileType, directory, path: prefix + entry.name, blobOid: entry.oid })
    }
  }
  return result
}

// Mirrors the retired REST probe so repos.security_file_enabled semantics don't shift
export function deriveSecurityFileEnabled(trees: RepoTrees): boolean {
  return [...(trees.root ?? []), ...(trees.github ?? [])].some(
    (entry) => entry.type === 'blob' && entry.name.toUpperCase() === 'SECURITY.MD',
  )
}
