import type { CustomFilterConfig } from '@/shared/modules/filters/types/filterTypes/CustomFilterConfig';
import type { Project } from '@/modules/lf/segments/types/Segments';

export interface ProjectsFilterValue {
  value: string[];
  parentValues: string[]
}

export interface SubProjectsOption {
  id: string;
  label: string;
  selected: boolean;
}

export interface ProjectsOption {
  id: string;
  label: string;
  selected: boolean;
  children: SubProjectsOption[];
}

export interface ProjectsCustomFilterOptions {
  remoteMethod: (value: {
    query: string;
    parentSlug: string | undefined;
  }) => Promise<Project[]>;
}

export interface ProjectsCustomFilterConfig extends CustomFilterConfig {
  options: ProjectsCustomFilterOptions;
}
