import type { MultiSelectFilterOptionGroup } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

const options: MultiSelectFilterOptionGroup[] = [
  {
    options: [
      {
        label: 'Silent',
        value: 'silent',
      },
      {
        label: 'Quiet',
        value: 'quiet',
      },
      {
        label: 'Engaged',
        value: 'engaged',
      },
      {
        label: 'Fan',
        value: 'fan',
      },
      {
        label: 'Ultra',
        value: 'ultra',
      },
    ],
  },
];

export default options;
