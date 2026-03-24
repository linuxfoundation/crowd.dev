import { FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import actor from './actor/config';
import entityId from './entityId/config';
import action from './action/config';

export const auditLogsFilters: Record<string, FilterConfig> = {
  entityId,
  actor,
  action,
};
