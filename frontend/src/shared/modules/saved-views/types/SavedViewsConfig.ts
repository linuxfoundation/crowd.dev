import type { Component } from 'vue';
import type { IncludeEnum } from '@/modules/member/config/saved-views/settings/common/types/IncludeEnum';
import type { Filter } from '@/shared/modules/filters/types/FilterConfig';
import type { ApiFilter } from '@/shared/modules/filters/types/FilterQuery';

export interface SavedView {
  id: string;
  name: string;
  visibility: string;
  placement: string;
  config: Filter;
  order?: number;
}

export interface SavedViewsSetting<T> {
  defaultValue: T;
  queryUrlParser: (value: string) => T;
  // method syntax lets SavedViewsSetting<IncludeEnum> be stored as SavedViewsSetting<unknown>
  apiFilterRenderer(value: T): ApiFilter[];
  inSettings: boolean;
  settingsComponent?: Component;
}

export type DefaultFiltersSettings = Record<'teamMember' | 'bot' | 'teamOrganization', IncludeEnum>;

export interface SavedViewsConfig {
  defaultView: SavedView;
  settings: Record<string, SavedViewsSetting<unknown>>;
  sorting: Record<string, string>; // Object with property and label for sorting
  defaultFilters: {
    render: (settings: DefaultFiltersSettings) => string;
  };
}

export interface SavedViewCreate {
  name: string;
  visibility: string;
  placement: string;
  config: Filter;
  order?: number;
}
