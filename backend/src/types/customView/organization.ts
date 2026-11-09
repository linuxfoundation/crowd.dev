import { CustomViewPlacement, CustomViewVisibility, ICustomView } from '@crowd/types'

const newAndActiveOrgs: ICustomView = {
  name: 'New and active',
  config: {
    search: '',
    relation: 'and',
    order: {
      prop: 'joinedAt',
      order: 'descending',
    },
    settings: {
      teamOrganization: 'exclude',
    },

    joinedDate: {
      operator: 'gt',
      value: 'lastMonth',
    },
  },
  visibility: CustomViewVisibility.TENANT,
  placement: CustomViewPlacement.ORGANIZATION,
}

const mostMembers: ICustomView = {
  name: 'Most contacts',
  config: {
    search: '',
    relation: 'and',
    order: {
      prop: 'memberCount',
      order: 'descending',
    },
    settings: {
      teamOrganization: 'exclude',
    },
  },
  visibility: CustomViewVisibility.TENANT,
  placement: CustomViewPlacement.ORGANIZATION,
}

const teamOrganizations: ICustomView = {
  name: 'Team companies',
  config: {
    search: '',
    relation: 'and',
    order: {
      prop: 'lastActive',
      order: 'descending',
    },
    settings: {
      teamOrganization: 'filter',
    },
  },
  visibility: CustomViewVisibility.TENANT,
  placement: CustomViewPlacement.ORGANIZATION,
}

export const organizationCustomViews: ICustomView[] = [
  newAndActiveOrgs,
  mostMembers,
  teamOrganizations,
]
