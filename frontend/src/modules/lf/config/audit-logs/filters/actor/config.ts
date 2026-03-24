import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { LfService } from '@/modules/lf/segments/lf-segments-service';
import {
  SelectAsyncFilterConfig, SelectAsyncFilterValue,
  SelectAsyncFilterOptions,
} from '@/shared/modules/filters/types/filterTypes/SelectAsyncFilterConfig';

const actor: SelectAsyncFilterConfig = {
  id: 'actor',
  label: 'Actor',
  iconClass: 'circle-user',
  type: FilterConfigType.SELECT_ASYNC,
  options: {
    hideIncludeSwitch: true,
    remoteMethod: (query) => LfService.fetchUsers(query, 10)
      .then((rows: any) => rows.map((actor: any) => ({
        label: actor.label,
        description: `${actor.email}`,
        value: actor.id,
      }))),
    remotePopulateItems: (id: string) => LfService.getUser(id)
      .then((actor: any) => ({
        label: actor.fullName,
        value: actor.id,
      })),
  },
  itemLabelRenderer(value: SelectAsyncFilterValue, options: SelectAsyncFilterOptions, data: any): string {
    return itemLabelRendererByType[FilterConfigType.SELECT_ASYNC]('Actor', value, options, data);
  },
  apiFilterRenderer({ value }: SelectAsyncFilterValue): any[] {
    return [{
      actorId: value,
    }];
  },
};

export default actor;
