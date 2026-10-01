import type {
  FilterDateOperator,
} from '@/shared/modules/filters/config/constants/date.constants';

export interface FilterTimeOptions {
  id: string;
  value: string;
  label: string;
  operator: FilterDateOperator;
  getDate: () => string;
}
