import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  BooleanFilterOptions,
  BooleanFilterValue,
} from '@/shared/modules/filters/types/filterTypes/BooleanFilterConfig';
import type {
  NumberFilterOptions,
  NumberFilterValue,
} from '@/shared/modules/filters/types/filterTypes/NumberFilterConfig';
import type {
  DateFilterOptions,
  DateFilterValue,
} from '@/shared/modules/filters/types/filterTypes/DateFilterConfig';
import type {
  SelectFilterOptions,
  SelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/SelectFilterConfig';
import type {
  MultiSelectFilterOptions,
  MultiSelectFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectFilterConfig';
import type {
  StringFilterOptions,
  StringFilterValue,
} from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import { stringItemLabelRenderer } from '@/shared/modules/filters/config/itemLabelRenderer/string.label.renderer';
import { multiSelectAsyncItemLabelRenderer } from '@/shared/modules/filters/config/itemLabelRenderer/multiselectasync.label.renderer';
import { selectAsyncItemLabelRenderer } from '@/shared/modules/filters/config/itemLabelRenderer/selectasync.label.renderer';
import { booleanItemLabelRenderer } from './itemLabelRenderer/boolean.label.renderer';
import { numberItemLabelRenderer } from './itemLabelRenderer/number.label.renderer';
import { dateItemLabelRenderer } from './itemLabelRenderer/date.label.renderer';
import { selectItemLabelRenderer } from './itemLabelRenderer/select.label.renderer';
import { multiSelectItemLabelRenderer } from './itemLabelRenderer/multiselect.label.renderer';

type ItemLabelRenderer<TValue, TOptions, TRenderOptions = unknown> = (
  property: string,
  value: TValue,
  options: TOptions,
  data?: unknown,
  renderOptions?: TRenderOptions,
) => string;

interface ItemLabelRendererByType {
  [FilterConfigType.BOOLEAN]: ItemLabelRenderer<BooleanFilterValue, BooleanFilterOptions>;
  [FilterConfigType.NUMBER]: ItemLabelRenderer<NumberFilterValue, NumberFilterOptions>;
  [FilterConfigType.DATE]: ItemLabelRenderer<DateFilterValue, DateFilterOptions>;
  [FilterConfigType.SELECT]: ItemLabelRenderer<
    SelectFilterValue,
    SelectFilterOptions,
    { addGroupLabel: boolean }
  >;
  [FilterConfigType.SELECT_ASYNC]: typeof selectAsyncItemLabelRenderer;
  [FilterConfigType.MULTISELECT]: ItemLabelRenderer<
    MultiSelectFilterValue,
    MultiSelectFilterOptions
  >;
  [FilterConfigType.MULTISELECT_ASYNC]: typeof multiSelectAsyncItemLabelRenderer;
  [FilterConfigType.STRING]: ItemLabelRenderer<StringFilterValue, StringFilterOptions>;
  [FilterConfigType.CUSTOM]: () => string;
}

export const itemLabelRendererByType: ItemLabelRendererByType = {
  [FilterConfigType.BOOLEAN]: booleanItemLabelRenderer,
  [FilterConfigType.NUMBER]: numberItemLabelRenderer,
  [FilterConfigType.DATE]: dateItemLabelRenderer,
  [FilterConfigType.SELECT]: selectItemLabelRenderer,
  [FilterConfigType.SELECT_ASYNC]: selectAsyncItemLabelRenderer,
  [FilterConfigType.MULTISELECT]: multiSelectItemLabelRenderer,
  [FilterConfigType.MULTISELECT_ASYNC]: multiSelectAsyncItemLabelRenderer,
  [FilterConfigType.STRING]: stringItemLabelRenderer,
  [FilterConfigType.CUSTOM]: () => '',
};
