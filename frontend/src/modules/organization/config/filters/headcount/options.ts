import type { MultiSelectFilterOptionGroup } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';

const options: MultiSelectFilterOptionGroup[] = [
  {
    options: [
      {
        label: '1-10',
        value: '1-10',
      },
      {
        label: '11-50',
        value: '11-50',
      },
      {
        label: '51-200',
        value: '51-200',
      },
      {
        label: '201-500',
        value: '201-500',
      },
      {
        label: '501-1000',
        value: '501-1000',
      },
      {
        label: '1001-5000',
        value: '1001-5001',
      },
      {
        label: '5001-10000',
        value: '5001-10000',
      },
      {
        label: '> 10000',
        value: '10000+',
      },
    ],
  },
];

export default options;
