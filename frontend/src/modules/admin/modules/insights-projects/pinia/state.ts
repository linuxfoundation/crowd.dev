import type { InsightsProjectModel } from '../models/insights-project.model';

export interface InsightsProjectsState {
  insightsProjects: InsightsProjectModel[];
}

const state: InsightsProjectsState = {
  insightsProjects: [],
};

export default () => state;
