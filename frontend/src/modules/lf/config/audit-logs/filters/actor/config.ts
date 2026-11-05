import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import { LfService } from '@/modules/lf/segments/lf-segments-service';
import type {
  SelectAsyncFilterConfig,
  SelectAsyncFilterValue,
  SelectAsyncFilterOptions,
  SelectAsyncFilterData,
} from '@/shared/modules/filters/types/filterTypes/SelectAsyncFilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';
import type { Actor } from '@/modules/lf/segments/types/AuditLog';

const actor: SelectAsyncFilterConfig = {
  id: 'actor',
  label: 'Actor',
  iconClass: 'circle-user',
  type: FilterConfigType.SELECT_ASYNC,
  options: {
    hideIncludeSwitch: true,
    remoteMethod: (query) =>
      LfService.fetchUsers(query, 10).then((rows: (Actor & { label: string })[]) =>
        rows.map((actor) => ({
          label: actor.label,
          description: `${actor.email}`,
          value: actor.id,
        })),
      ),
    remotePopulateItems: (id: string) =>
      LfService.getUser(id).then((actor: Actor) => ({
        label: actor.fullName,
        value: actor.id,
      })),
  },
  itemLabelRenderer(
    value: SelectAsyncFilterValue,
    options: SelectAsyncFilterOptions,
    data: SelectAsyncFilterData,
  ): string {
    return itemLabelRendererByType[FilterConfigType.SELECT_ASYNC]('Actor', value, options, data);
  },
  apiFilterRenderer({ value }: SelectAsyncFilterValue): ApiFilter[] {
    return [
      {
        actorId: value,
      },
    ];
  },
};

export default actor;
