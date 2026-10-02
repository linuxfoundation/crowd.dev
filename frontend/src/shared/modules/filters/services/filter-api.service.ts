import type { Filter, FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import type { SearchFilterConfig } from '@/shared/modules/filters/types/filterTypes/SearchFilterConfig';
import type { FilterQuery } from '@/shared/modules/filters/types/FilterQuery';
import type { SavedViewsConfig } from '@/shared/modules/saved-views/types/SavedViewsConfig';

export const filterApiService = () => {
  function buildApiFilter(
    values: Filter,
    configuration: Record<string, FilterConfig>,
    searchConfig?: SearchFilterConfig,
    savedViewsConfig?: SavedViewsConfig,
  ): FilterQuery {
    const {
      search,
      relation,
      order,
      settings,
      ...filterValues
    } = values;

    let baseFilters: any[] = [];
    let filters: any[] = [];
    let body: any = {};

    // Search
    if (search && search.length > 0) {
      baseFilters = [
        ...baseFilters,
        ...(searchConfig?.apiFilterRenderer ? searchConfig.apiFilterRenderer(search) : []),
      ];
    }

    // Settings
    if (savedViewsConfig && settings) {
      Object.entries(settings).forEach(([setting, value]) => {
        const config: FilterConfig = configuration[setting];

        if (config?.inBody) {
          return;
        }

        const filter = savedViewsConfig.settings[setting]?.apiFilterRenderer(value);
        if (filter) {
          baseFilters = [
            ...baseFilters,
            ...filter,
          ];
        }
      });
    }

    // Filter values
    Object.entries(filterValues).forEach(([key, values]) => {
      const config: FilterConfig = configuration[key];
      if (!config || config.inBody) {
        return;
      }
      const filter = config?.apiFilterRenderer(values);
      if (filter && filter.length > 0) {
        filters = [
          ...filters,
          ...filter,
        ];
      }
    });

    // In body filters
    Object.entries(filterValues).forEach(([key, values]) => {
      const config: FilterConfig = configuration[key];
      if (!config || !config.inBody) {
        return;
      }
      const filter: any[] = configuration[key]?.apiFilterRenderer(values) || [];
      filter.forEach((obj) => {
        body = {
          ...body,
          ...obj,
        };
      });
    });

    // build object
    let filter = {
      [relation]: filters.length > 0 ? filters : undefined,
    };
    if (baseFilters.length > 0) {
      filter = {
        and: [
          ...baseFilters,
          filter,
        ],
      };
    }

    const orderBy = `${order.prop}_${order.order === 'descending' ? 'DESC' : 'ASC'}`;

    return {
      search,
      filter,
      orderBy,
      body,
    };
  }

  return {
    buildApiFilter,
  };
};
