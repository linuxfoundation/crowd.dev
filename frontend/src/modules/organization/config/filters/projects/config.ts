import { FilterConfigType } from '@/shared/modules/filters/types/FilterConfig';
import ProjectsFilter from '@/modules/organization/config/filters/projects/ProjectsFilter.vue';
import { LfService } from '@/modules/lf/segments/lf-segments-service';
import type { Project } from '@/modules/lf/segments/types/Segments';
import type { ProjectsFilterValue, ProjectsCustomFilterConfig } from '@/modules/lf/segments/types/Filters';
import { filterLabel } from '@/modules/lf/utils/filters';

const projects: ProjectsCustomFilterConfig = {
  id: 'projects',
  label: 'Project group',
  iconClass: 'layer-group',
  inBody: true,
  type: FilterConfigType.CUSTOM,
  component: ProjectsFilter,
  options: {
    remoteMethod: ({
      query,
      parentSlug,
    }) => LfService.queryProjects({
      limit: 10,
      filter: {
        name: query,
        parentSlug,
      },
    })
      .then(({ rows }) => rows),
  },
  queryUrlParser({ value }: {value: string}): Record<string, string[]> {
    return {
      value: value.split(','),
    };
  },
  itemLabelRenderer({ value, parentValues }: ProjectsFilterValue, options: any, data: { options: Project[] }): string {
    const charLimit = 30;
    const text = filterLabel(value, parentValues, data.options);
    const trimmedValueText = text.length > charLimit ? `${text.substring(0, charLimit - 3)}...` : text;
    const tooltip = trimmedValueText.length < text.length ? `data-tooltip="${text}"` : '';

    return `<span ${tooltip}><b>Project group:</b>${trimmedValueText || '...'}</span>`;
  },
  apiFilterRenderer({ value }: ProjectsFilterValue): any[] {
    return [
      { segments: value },
    ];
  },
};

export default projects;
