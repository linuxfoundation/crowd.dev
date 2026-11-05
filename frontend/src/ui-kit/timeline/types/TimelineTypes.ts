import type { RouteLocationRaw } from 'vue-router';

export interface TimelineGroup<T = unknown> {
  id: number;
  label: string;
  labelLink?: RouteLocationRaw;
  icon?: string;
  items: T[];
}
