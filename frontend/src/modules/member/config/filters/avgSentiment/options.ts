import type { MultiSelectFilterOptionGroup } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

const options: MultiSelectFilterOptionGroup[] = [
  {
    options: [
      {
        label: 'Positive',
        value: 'positive',
      },
      {
        label: 'Neutral',
        value: 'neutral',
      },
      {
        label: 'Negative',
        value: 'negative',
      },
    ],
  },
];

export default options;
