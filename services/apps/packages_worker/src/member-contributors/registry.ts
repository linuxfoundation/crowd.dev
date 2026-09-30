export interface AkritesMember {
  name: string
  cdpOrganizationIds: string[]
}

export const AKRITES_MEMBERS: AkritesMember[] = [
  {
    name: 'AWS',
    cdpOrganizationIds: [
      'b0eb87bd-e9e2-44d3-b298-2dec45ac1d7f',
      'ebe57650-afd6-11ee-977c-abda62085fa6',
    ],
  },
  { name: 'Anthropic', cdpOrganizationIds: ['89f029ab-babe-4e7a-9e1d-589b160459e3'] },
  { name: 'Chainguard', cdpOrganizationIds: ['2b03a4d7-cd01-4892-a8eb-7f214663979e'] },
  {
    name: 'Cisco',
    cdpOrganizationIds: [
      '8a96bca0-3642-11ee-9261-0dbe04a00eff',
      '16fd1447-79f5-4256-af05-f8cb02825b75',
    ],
  },
  { name: 'Endor Labs', cdpOrganizationIds: ['98d16561-4611-4945-9dbc-0ef7ed1b35f1'] },
  {
    name: 'Ericsson',
    cdpOrganizationIds: [
      'c806a6be-93c0-4e65-bb36-fb48663dd1df',
      '074d0b00-2a4e-11ef-86da-9b06e86c291c',
    ],
  },
  { name: 'Google', cdpOrganizationIds: ['5988fba3-0dd3-4302-995f-6448fd307af2'] },
  { name: 'HeroDevs', cdpOrganizationIds: ['c6db69c0-8dfb-11ee-a1a0-2b2bcb4b41da'] },
  {
    name: 'IBM',
    cdpOrganizationIds: [
      'd55d6a90-6739-11ee-b995-d56289625d87',
      'f7fb93c3-4d76-4d53-8d4a-747106ca65fa',
    ],
  },
  {
    name: 'JPMorgan Chase',
    cdpOrganizationIds: [
      '52753bb9-6a3a-4ccf-9851-e494059ad279',
      '70f16a6f-b7c2-4a64-a664-33f1f266958b',
      'dcc068c7-6077-4f41-9e38-c8b51ea6133f',
    ],
  },
  { name: 'Microsoft', cdpOrganizationIds: ['964170f2-f012-424e-a94a-cd9a4d799e6a'] },
  { name: 'NVIDIA', cdpOrganizationIds: ['2521a27d-fb94-4620-a51d-417774e96b02'] },
  { name: 'Palo Alto Networks', cdpOrganizationIds: ['5cae9f38-f818-40b2-b076-6f7b140685de'] },
  { name: 'RapidFort', cdpOrganizationIds: ['e2a48c85-a3d8-46c0-a203-75f60757a10f'] },
  {
    name: 'Red Hat',
    cdpOrganizationIds: [
      'd7bf07d9-780b-4279-bfbc-d83f85950353',
      '11fced09-711d-47fb-a8fd-f21f65828594',
    ],
  },
  { name: 'Sonatype', cdpOrganizationIds: ['1d175818-1f30-4c86-ba01-cc71c5899f36'] },
  {
    name: 'Vodafone',
    cdpOrganizationIds: [
      '5b53dda0-0f71-11f1-ba0f-132239d9c021',
      '4096ce16-0261-4e1f-8146-3e7d6a185c1e',
    ],
  },
]

export function indexMembersByCdpOrganizationId(
  members: AkritesMember[] = AKRITES_MEMBERS,
): Map<string, AkritesMember> {
  const index = new Map<string, AkritesMember>()
  for (const member of members) {
    for (const organizationId of member.cdpOrganizationIds) {
      index.set(organizationId, member)
    }
  }
  return index
}
