import {
  IdentityKey,
  findAffiliationsForIdentityKeys,
} from '@crowd/data-access-layer/src/osspckgs/identityAffiliations'
import {
  InferredRepoMemberContributorsBody,
  groupAffiliatedMemberIdentities,
  toInferredRepoMemberContributorsBody,
} from '@crowd/data-access-layer/src/osspckgs/memberContributorsBody'

import { getPackagesDb } from '../../db'

export interface BuildInferredRepoBodyInput {
  purl: string
  ecosystem: string
  repoUrl: string
  logins: string[]
}

export function purlPackageName(purl: string): string {
  const afterScheme = purl.slice('pkg:'.length)
  const afterType = afterScheme.slice(afterScheme.indexOf('/') + 1)
  const withoutQualifiers = afterType.split(/[?#]/)[0]
  const withoutVersion = withoutQualifiers.replace(/@[^/@]+$/, '')
  return decodeURIComponent(withoutVersion)
}

function toLoginKeys(logins: string[]): IdentityKey[] {
  return logins.map((identityValue) => ({ identityType: 'github-login', identityValue }))
}

export async function buildInferredRepoMemberContributors(
  input: BuildInferredRepoBodyInput,
): Promise<InferredRepoMemberContributorsBody> {
  const rows =
    input.logins.length === 0
      ? []
      : await findAffiliationsForIdentityKeys(await getPackagesDb(), toLoginKeys(input.logins))
  return toInferredRepoMemberContributorsBody(
    {
      purl: input.purl,
      name: purlPackageName(input.purl),
      ecosystem: input.ecosystem,
      repoUrl: input.repoUrl,
    },
    groupAffiliatedMemberIdentities(rows),
  )
}
