import type { MultiSelectFilterOptionGroup } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

const options: MultiSelectFilterOptionGroup[] = [
  {
    options: [
      {
        label: 'Maintainer',
        value: 'maintainer',
      },
      {
        label: 'Contributor',
        value: 'contributor',
      },
    ],
  },
];

export default options;
