import type { SelectFilterOptionGroup } from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';

const options: SelectFilterOptionGroup[] = [
  {
    label: 'Person',
    options: [
      {
        label: 'Profiles merged',
        value: 'members-merge',
      },
      {
        label: 'Profiles unmerged',
        value: 'members-unmerge',
      },
      {
        label: 'Profile identities updated',
        value: 'members-edit-identities',
      },
      {
        label: 'Profile work experience updated',
        value: 'members-edit-organizations',
      },
      {
        label: 'Profile affiliation updated',
        value: 'members-edit-manual-affiliation',
      },
      {
        label: 'Profile updated',
        value: 'members-edit-profile',
      },
      {
        label: 'Profile created',
        value: 'members-create',
      },
    ],
  },
  {
    label: 'Organization',
    options: [
      {
        label: 'Organizations merged',
        value: 'organizations-merged',
      },
      {
        label: 'Organizations unmerged',
        value: 'organizations-unmerged',
      },
      {
        label: 'Organization identities updated',
        value: 'organizations-edit-identities',
      },
      {
        label: 'Organization profile updated',
        value: 'organizations-edit-profile',
      },
      {
        label: 'Organization created',
        value: 'organizations-create',
      },
    ],
  },
  {
    label: 'Integration',
    options: [
      {
        label: 'Integration connected',
        value: 'integrations-connect',
      },
      {
        label: 'Integration re-connected',
        value: 'integrations-reconnect',
      },
    ],
  },
];

export default options;
