import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type { Component } from 'vue';
import BooleanFilter from '@/shared/modules/filters/components/filterTypes/BooleanFilter.vue';
import MultiSelectFilter from '@/shared/modules/filters/components/filterTypes/MultiSelectFilter.vue';
import SelectFilter from '@/shared/modules/filters/components/filterTypes/SelectFilter.vue';
import SelectAsyncFilter from '@/shared/modules/filters/components/filterTypes/SelectAsyncFilter.vue';
import DateFilter from '@/shared/modules/filters/components/filterTypes/DateFilter.vue';
import NumberFilter from '@/shared/modules/filters/components/filterTypes/NumberFilter.vue';
import StringFilter from '@/shared/modules/filters/components/filterTypes/StringFilter.vue';
import MultiSelectAsyncFilter from '@/shared/modules/filters/components/filterTypes/MultiSelectAsyncFilter.vue';

export const filterComponentByType: Record<FilterConfigType, Component | null> = {
  [FilterConfigType.BOOLEAN]: BooleanFilter,
  [FilterConfigType.NUMBER]: NumberFilter,
  [FilterConfigType.DATE]: DateFilter,
  [FilterConfigType.SELECT]: SelectFilter,
  [FilterConfigType.SELECT_ASYNC]: SelectAsyncFilter,
  [FilterConfigType.MULTISELECT]: MultiSelectFilter,
  [FilterConfigType.MULTISELECT_ASYNC]: MultiSelectAsyncFilter,
  [FilterConfigType.STRING]: StringFilter,
  [FilterConfigType.CUSTOM]: null,
};
