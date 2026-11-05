// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { InsightsProjectModel } from '../models/insights-project.model';

export const useInsightsProjectsStore = defineStore('insights-projects-store', () => {
  const insightsProjects = ref<InsightsProjectModel[]>([]);

  function setInsightsProjects(projects: InsightsProjectModel[]) {
    insightsProjects.value = projects;
  }

  function getInsightsProject(id: string) {
    return insightsProjects.value.find((c) => c.id === id);
  }

  function getInsightsProjects() {
    return insightsProjects.value;
  }

  function updateInsightsProject(project: InsightsProjectModel) {
    const index = insightsProjects.value.findIndex((c) => c.id === project.id);
    if (index !== -1) {
      insightsProjects.value[index] = project;
    }
  }

  function createInsightsProject(project: InsightsProjectModel) {
    insightsProjects.value.push(project);
  }

  function searchInsightsProjects(query: string) {
    return insightsProjects.value.filter((c) => c.name.toLowerCase().includes(query.toLowerCase()));
  }

  return {
    insightsProjects,
    setInsightsProjects,
    getInsightsProject,
    getInsightsProjects,
    updateInsightsProject,
    createInsightsProject,
    searchInsightsProjects,
  };
});
