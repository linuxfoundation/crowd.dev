import { IMemberMergeSuggestion } from '@crowd/types'

import { ISubprojectMember, ISubprojectMemberMergePair } from './types'

// Postgres already found likely duplicate pairs in this project.
// We decide why they matched and how confident to be.
export function scoreSubprojectMemberMergePairs(
  pairs: ISubprojectMemberMergePair[],
): IMemberMergeSuggestion[] {
  const suggestions: IMemberMergeSuggestion[] = []

  for (const pair of pairs) {
    // two verified usernames on the same platform = two different people
    if (hasVerifiedUsernameClash(pair.primary, pair.other)) {
      continue
    }

    const rules = classifyPair(pair.primary, pair.other)
    if (rules.length === 0) {
      continue
    }

    const complementary = isComplementary(pair.primary, pair.other)
    const hasUsernameSignal = rules.some(isUsernameRule)

    // git-only vs github-only is the onboarding split we care about most
    let similarity = 0.8
    if (complementary && hasUsernameSignal) {
      similarity = 0.9
    } else if (complementary) {
      similarity = 0.85
    }

    suggestions.push({
      similarity,
      activityEstimate: pair.primary.activityCount + pair.other.activityCount,
      members: [pair.primary.id, pair.other.id],
    })
  }

  return suggestions
}

function classifyPair(left: ISubprojectMember, right: ISubprojectMember): Rule[] {
  const leftView = memberView(left)
  const rightView = memberView(right)
  const rules: Rule[] = []

  // "Jane Doe" === "Jane Doe"
  if (
    leftView.normName.length >= 4 &&
    leftView.normName === rightView.normName &&
    !leftView.isPlaceholder
  ) {
    rules.push('same_display_name')
  }

  // "Zhou Shelven" vs "Shelven Zhou"
  if (
    leftView.normName.includes(' ') &&
    leftView.sortedTokens === rightView.sortedTokens &&
    leftView.normName !== rightView.normName
  ) {
    rules.push('reversed_name')
  }

  // git display name "fishyu-mushroom" vs github username fishyu-mushroom
  if (hasDisplayNameUsernameMatch(leftView, rightView)) {
    rules.push('display_name_username')
  }

  // git email fishyu@... vs github username fishyu
  if (hasEmailLocalpartUsernameMatch(leftView, rightView)) {
    rules.push('email_localpart_username')
  }

  return rules
}

type Rule =
  | 'same_display_name'
  | 'reversed_name'
  | 'display_name_username'
  | 'email_localpart_username'

type MemberView = {
  normName: string
  compactName: string
  sortedTokens: string
  isPlaceholder: boolean
  usernames: string[]
  emailLocalParts: string[]
  platforms: string[]
  identities: ISubprojectMember['identities']
}

function memberView(member: ISubprojectMember): MemberView {
  const displayName = member.displayName ?? ''
  const normName = displayName.trim().replace(/\s+/g, ' ').toLowerCase()
  const identities = member.identities ?? []
  const usernames: string[] = []
  const emailLocalParts: string[] = []
  const platforms = new Set<string>()

  for (const identity of identities) {
    platforms.add(identityPlatform(identity.platform))
    const value = identity.value.toLowerCase()
    if (
      identity.type === 'username' &&
      identityPlatform(identity.platform) !== 'git' &&
      !value.includes('@') &&
      value.length >= 5
    ) {
      usernames.push(value)
    }
    const at = value.indexOf('@')
    if (at >= 1) {
      const localPart = value.slice(0, at)
      if (localPart.length >= 5) {
        emailLocalParts.push(localPart)
      }
    }
  }

  return {
    normName,
    compactName: displayName.replace(/[^a-zA-Z0-9]+/g, '').toLowerCase(),
    sortedTokens: normName.split(' ').filter(Boolean).sort().join(' '),
    isPlaceholder: ['unknown', 'root', 'ubuntu', 'admin', 'user', 'guest'].includes(normName),
    usernames,
    emailLocalParts,
    platforms: [...platforms],
    identities,
  }
}

function hasDisplayNameUsernameMatch(left: MemberView, right: MemberView): boolean {
  if (left.compactName.length < 5 && right.compactName.length < 5) {
    return false
  }
  return nameMatchesUsername(left, right.usernames) || nameMatchesUsername(right, left.usernames)
}

function nameMatchesUsername(member: MemberView, usernames: string[]): boolean {
  if (member.compactName.length < 5) {
    return false
  }
  return usernames.includes(member.normName) || usernames.includes(member.compactName)
}

function hasEmailLocalpartUsernameMatch(left: MemberView, right: MemberView): boolean {
  return (
    left.emailLocalParts.some((localPart) => right.usernames.includes(localPart)) ||
    right.emailLocalParts.some((localPart) => left.usernames.includes(localPart))
  )
}

function identityPlatform(platform: string): string {
  // github-nango identities are still github
  return platform === 'github-nango' ? 'github' : platform
}

function isUsernameRule(rule: Rule): boolean {
  switch (rule) {
    case 'display_name_username':
    case 'email_localpart_username':
      return true
    case 'same_display_name':
    case 'reversed_name':
      return false
    default: {
      const exhaustive: never = rule
      return exhaustive
    }
  }
}

function isComplementary(left: ISubprojectMember, right: ISubprojectMember): boolean {
  const leftPlatforms = new Set(
    (left.identities ?? []).map((identity) => identityPlatform(identity.platform)),
  )
  const rightPlatforms = new Set(
    (right.identities ?? []).map((identity) => identityPlatform(identity.platform)),
  )
  const leftGitOnly = leftPlatforms.has('git') && !leftPlatforms.has('github')
  const rightGitOnly = rightPlatforms.has('git') && !rightPlatforms.has('github')
  const leftGithubOnly = leftPlatforms.has('github') && !leftPlatforms.has('git')
  const rightGithubOnly = rightPlatforms.has('github') && !rightPlatforms.has('git')

  return (leftGitOnly && rightGithubOnly) || (rightGitOnly && leftGithubOnly)
}

function hasVerifiedUsernameClash(left: ISubprojectMember, right: ISubprojectMember): boolean {
  for (const leftIdentity of left.identities ?? []) {
    if (leftIdentity.type !== 'username' || !leftIdentity.verified) {
      continue
    }
    const platform = identityPlatform(leftIdentity.platform)
    if (platform === 'git') {
      continue
    }
    for (const rightIdentity of right.identities ?? []) {
      if (
        rightIdentity.type === 'username' &&
        rightIdentity.verified &&
        identityPlatform(rightIdentity.platform) === platform &&
        rightIdentity.value.toLowerCase() !== leftIdentity.value.toLowerCase()
      ) {
        return true
      }
    }
  }
  return false
}
