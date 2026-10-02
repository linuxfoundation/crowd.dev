import type { IncludeEnum } from '@/modules/member/config/saved-views/settings/common/types/IncludeEnum';
import type { Filter } from '@/shared/modules/filters/types/FilterConfig';

export interface SavedView {
  id: string;
  name: string;
  visibility: string;
  placement: string;
  config: Filter & Record<string, any>;
  order?: number;
}

export interface SavedViewsSetting<T> {
  defaultValue: T;
  queryUrlParser: (value: string) => T;
  apiFilterRenderer: (value: T) => any[];
  inSettings: boolean;
  settingsComponent?: any;
}

export type DefaultFiltersSettings = Record<'teamMember' | 'bot' | 'teamOrganization', IncludeEnum>

export interface SavedViewsConfig {
  defaultView: SavedView;
  settings: Record<string, SavedViewsSetting<any>>;
  sorting: Record<string, string>; // Object with property and label for sorting
  defaultFilters: {
    render: (
      settings: DefaultFiltersSettings
    ) => string;
  }
}

export interface SavedViewCreate {
  name: string;
  visibility: string;
  placement: string;
  config: Filter & Record<string, any>;
  order?: number;
}
