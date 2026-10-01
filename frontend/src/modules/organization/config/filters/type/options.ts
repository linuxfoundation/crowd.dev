import type { MultiSelectFilterOptionGroup } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

const options: MultiSelectFilterOptionGroup[] = [
  {
    options: [
      {
        label: 'Educational',
        value: 'educational',
      },
      {
        label: 'Government',
        value: 'government',
      },
      {
        label: 'Nonprofit',
        value: 'nonprofit',
      },
      {
        label: 'Private',
        value: 'private',
      },
      {
        label: 'Public',
        value: 'public',
      },
    ],
  },
];

export default options;
