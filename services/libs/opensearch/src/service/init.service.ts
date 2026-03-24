import {
  IMemberWithAggregatesForMergeSuggestions,
  IOrganizationFullAggregatesOpensearch,
  MemberIdentityType,
  OrganizationIdentityType,
} from '@crowd/types'

import { OpenSearchIndex } from '../types'

import { MemberSyncService } from './member.sync.service'
import { OpenSearchService } from './opensearch.service'
import { OrganizationSyncService } from './organization.sync.service'

export class InitService {
  public static FAKE_SEGMENT_ID = 'ce36b0b0-1fc4-4637-955d-afb8a6b58e48'
  public static FAKE_MEMBER_ID = '9c19e17c-6a07-4f4c-bc9b-ce1fdce9c126'
  public static FAKE_ACTIVITY_ID = 'fa761640-f77c-4340-b56e-bdd0936d852b'
  public static FAKE_CONVERSATION_ID = 'cba1758c-7b1f-4a3c-b6ff-e6f3bdf54c86'
  public static FAKE_ORGANIZATION_ID = 'cba1758c-7b1f-4a3c-b6ff-e6f3bdf54c85'

  constructor(private readonly openSearchService: OpenSearchService) {}

  public async initialize(): Promise<void> {
    await this.openSearchService.initialize()

    await this.createFakeMember()
    await this.createFakeOrganization()
  }

  private async createFakeOrganization(): Promise<void> {
    const fakeOrg: IOrganizationFullAggregatesOpensearch = {
      id: InitService.FAKE_ORGANIZATION_ID,
      noMergeIds: [],
      website: 'test.com',
      ticker: 'FAKE',
      displayName: 'Fake organization',
      industry: 'Fake industry',
      location: 'Unknown City, Unknown Country',
      activityCount: 10,
      identities: [
        {
          platform: 'devto',
          value: 'fakeorg',
          type: OrganizationIdentityType.USERNAME,
          verified: true,
          source: null,
          sourceId: null,
          integrationId: null,
        },
      ],
    }

    const prepared = OrganizationSyncService.prefixData(fakeOrg)
    await this.openSearchService.index(
      `${InitService.FAKE_ORGANIZATION_ID}-${InitService.FAKE_SEGMENT_ID}`,
      OpenSearchIndex.ORGANIZATIONS,
      prepared,
    )
  }

  private async createFakeMember(): Promise<void> {
    // we need to create a fake member to initialize the index with the proper data
    // it will be created in a nonexisting tenant so no one will see it ever
    // if we don't have anything in the index any search by any field will return an error

    const fakeMember: IMemberWithAggregatesForMergeSuggestions = {
      id: InitService.FAKE_MEMBER_ID,
      displayName: 'Test Member',

      activityCount: 10,
      identities: [
        {
          id: 'fake-identity-1',
          memberId: InitService.FAKE_MEMBER_ID,
          platform: 'devto',
          value: 'Test Member',
          type: MemberIdentityType.USERNAME,
          verified: true,
          source: null,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        },
        {
          id: 'fake-identity-2',
          memberId: InitService.FAKE_MEMBER_ID,
          platform: 'github',
          value: 'fakeWeakIdentity',
          type: MemberIdentityType.USERNAME,
          verified: false,
          source: null,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        },
        {
          id: 'fake-identity-3',
          memberId: InitService.FAKE_MEMBER_ID,
          platform: 'github',
          value: 'test@email.com',
          type: MemberIdentityType.EMAIL,
          verified: true,
          source: null,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        },
      ],
      organizations: [
        {
          id: '0dfaa9a0-d95a-4397-958e-4727189e3ef8',
          memberId: InitService.FAKE_MEMBER_ID,
          organizationId: 'b176d053-c53e-42d2-88d2-6fbc3e34184c',
          displayName: 'Test Organization',
          title: 'blabla',
          dateStart: new Date().toISOString(),
          dateEnd: new Date().toISOString(),
        },
      ],
      attributes: {},
    }

    const prepared = MemberSyncService.prefixData(fakeMember, [])
    await this.openSearchService.index(
      `${InitService.FAKE_MEMBER_ID}`,
      OpenSearchIndex.MEMBERS,
      prepared,
    )
  }
}
