import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import { itemLabelRendererByType } from '@/shared/modules/filters/config/itemLabelRendererByType';
import type {
  MultiSelectAsyncFilterOptions, MultiSelectAsyncFilterValue,
} from '@/shared/modules/filters/types/filterTypes/MultiSelectAsyncFilterConfig';
import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import { DEFAULT_ORGANIZATION_FILTERS } from '@/modules/organization/store/constants';
import { OrganizationService } from '@/modules/organization/organization-service';
import { trimAndReduceSpaces } from '@/utils/string';
import { queryUrlParserByType } from '@/shared/modules/filters/config/queryUrlParserByType';
import type { Organization } from '@/modules/organization/types/Organization';
import OrganizationsFilter from './OrganizationsFilter.vue';

const organizations: CustomFilterConfig = {
  id: 'organizations',
  label: 'Organization',
  iconClass: 'building',
  type: FilterConfigType.CUSTOM,
  component: OrganizationsFilter,
  options: {
    remoteMethod: (query: string) => OrganizationService.query({
      filter: {
        or: [
          { displayName: { textContains: trimAndReduceSpaces(query) } },
        ],
      },
      limit: 10,
      segments: [],
    })
      .then(({ rows }: { rows: Organization[] }) => rows.map((organization) => ({
        label: organization.displayName || organization.name,
        lfxMembership: organization.lfxMembership,
        value: organization.id,
        prefix: `
            <span class="flex items-center justify-center w-6 h-6 p-1 mr-3 border rounded-md ">
            ${organization.logo
          ? `<img src="${organization.logo}" class="w-4 h-4 min-w-[16px]" alt="${organization.displayName}" />`
          : '<i v-else class="flex items-center justify-center w-4 h-4 text-gray-300 fa-light fa-building c-icon" />'}
            </span>`,
      }))),
    remotePopulateItems: (ids: string[]) => OrganizationService.query({
      filter: {
        and: [
          ...DEFAULT_ORGANIZATION_FILTERS,
          {
            id: { in: ids },
          },
        ],
      },
      limit: ids?.length,
      segments: [],
    })
      .then(({ rows }: any) => rows.map((organization: any) => ({
        label: organization.displayName,
        lfxMembership: organization.lfxMembership,
        value: organization.id,
        logo: organization.logo,
      }))),
  },
  queryUrlParser: queryUrlParserByType[FilterConfigType.MULTISELECT_ASYNC],
  itemLabelRenderer(value: MultiSelectAsyncFilterValue, options: MultiSelectAsyncFilterOptions, data: any): string {
    return itemLabelRendererByType[FilterConfigType.MULTISELECT_ASYNC]('Organization', value, options, data);
  },
  apiFilterRenderer({ value, include }: MultiSelectAsyncFilterValue): any[] {
    const filter = {
      or: value.map((id) => ({ organizations: { contains: [id] } })),
    };
    return [
      (include ? filter : { not: filter }),
    ];
  },
};

export default organizations;
