import type { InsightsProjectModel } from '../models/insights-project.model';
import type { InsightsProjectsState } from './state';

export default {
  setInsightsProjects(
    this: InsightsProjectsState,
    insightsProjects: InsightsProjectModel[],
  ) {
    this.insightsProjects = insightsProjects;
  },
  getInsightsProject(this: InsightsProjectsState, id: string) {
    return this.insightsProjects.find((c) => c.id === id);
  },
  getInsightsProjects(this: InsightsProjectsState) {
    return this.insightsProjects;
  },
  updateInsightsProject(
    this: InsightsProjectsState,
    project: InsightsProjectModel,
  ) {
    const index = this.insightsProjects.findIndex((c) => c.id === project.id);
    if (index !== -1) {
      this.insightsProjects[index] = project;
    }
  },
  createInsightsProject(
    this: InsightsProjectsState,
    project: InsightsProjectModel,
  ) {
    this.insightsProjects.push(project);
  },
  searchInsightsProjects(this: InsightsProjectsState, query: string) {
    return this.insightsProjects.filter((c) => c.name.toLowerCase().includes(query.toLowerCase()));
  },
};
