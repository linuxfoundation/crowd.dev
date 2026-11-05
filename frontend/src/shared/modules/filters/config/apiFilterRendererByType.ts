import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import type { BooleanFilterValue } from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import type { NumberFilterValue } from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import type { DateFilterValue } from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import type { SelectFilterValue } from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';
import type { SelectAsyncFilterValue } from '@/shared/modules/filters/types/filterTypes/SelectAsyncFilterConfig';
import type { MultiSelectFilterValue } from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import type { MultiSelectAsyncFilterValue } from '@/shared/modules/filters/types/filterTypes/MultiSelectAsyncFilterConfig';
import type { StringFilterValue } from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import { stringApiFilterRenderer } from '@/shared/modules/filters/config/apiFilterRenderer/string.filter.renderer';
import { booleanApiFilterRenderer } from './apiFilterRenderer/boolean.filter.renderer';
import { numberApiFilterRenderer } from './apiFilterRenderer/number.filter.renderer';
import { dateApiFilterRenderer } from './apiFilterRenderer/date.filter.renderer';
import { selectApiFilterRenderer } from './apiFilterRenderer/select.filter.renderer';
import { multiSelectApiFilterRenderer } from './apiFilterRenderer/multiselect.filter.renderer';

type ApiFilterRenderer<TValue> = (
  property: string,
  value: TValue,
  parse?: (value: number) => number,
) => ApiFilter[];

interface ApiFilterRendererByType {
  [FilterConfigType.BOOLEAN]: ApiFilterRenderer<BooleanFilterValue>;
  [FilterConfigType.NUMBER]: ApiFilterRenderer<NumberFilterValue>;
  [FilterConfigType.DATE]: ApiFilterRenderer<DateFilterValue>;
  [FilterConfigType.SELECT]: ApiFilterRenderer<SelectFilterValue>;
  [FilterConfigType.SELECT_ASYNC]: ApiFilterRenderer<SelectAsyncFilterValue>;
  [FilterConfigType.MULTISELECT]: ApiFilterRenderer<MultiSelectFilterValue>;
  [FilterConfigType.MULTISELECT_ASYNC]: ApiFilterRenderer<MultiSelectAsyncFilterValue>;
  [FilterConfigType.STRING]: ApiFilterRenderer<StringFilterValue>;
  [FilterConfigType.CUSTOM]: () => ApiFilter[];
}

export const apiFilterRendererByType: ApiFilterRendererByType = {
  [FilterConfigType.BOOLEAN]: booleanApiFilterRenderer,
  [FilterConfigType.NUMBER]: numberApiFilterRenderer,
  [FilterConfigType.DATE]: dateApiFilterRenderer,
  [FilterConfigType.SELECT]: selectApiFilterRenderer,
  [FilterConfigType.SELECT_ASYNC]: selectApiFilterRenderer,
  [FilterConfigType.MULTISELECT]: multiSelectApiFilterRenderer,
  [FilterConfigType.MULTISELECT_ASYNC]: multiSelectApiFilterRenderer,
  [FilterConfigType.STRING]: stringApiFilterRenderer,
  [FilterConfigType.CUSTOM]: (): ApiFilter[] => [],
};
