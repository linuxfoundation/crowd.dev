import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import type {
  StringFilterConfig,
  StringFilterOptions,
  StringFilterValue,
} from '@/shared/modules/filters/types/filterTypes/StringFilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { FilterStringOperator } from '@/shared/modules/filters/config/constants/string.constants';

const entityId: StringFilterConfig = {
  id: 'entityId',
  label: 'Entity Id',
  iconClass: 'shapes',
  type: FilterConfigType.STRING,
  options: {
    fixedOperator: FilterStringOperator.EQ,
  },
  itemLabelRenderer(value: StringFilterValue, options: StringFilterOptions): string {
    return itemLabelRendererByType[FilterConfigType.STRING]('Entity Id', value, options);
  },
  apiFilterRenderer(value: StringFilterValue): any[] {
    return [{
      entityId: value.value,
    }];
  },
};

export default entityId;
