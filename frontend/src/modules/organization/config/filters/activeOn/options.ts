import type { MultiSelectFilterOptionGroup } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

const options: MultiSelectFilterOptionGroup[] = [
  {
    options: [
      {
        label: 'Github',
        value: 'github',
      },
      {
        label: 'LinkedIn',
        value: 'linkedin',
      },
      {
        label: 'X/Twitter',
        value: 'twitter',
      },
      {
        label: 'Crunchbase',
        value: 'crunchbase',
      },
    ],
  },
];

export default options;
