import { defineStore } from 'pinia';
import { ref } from 'vue';
import { Project, ProjectGroup, SubProject } from '@/modules/lf/segments/types/Segments';

export const useOverviewStore = defineStore('overview', () => {
  const selectedProjectGroupId = ref<string>('');
  const selectedProjectGroup = ref<ProjectGroup | null>(null);
  const selectedProjectId = ref<string>('');
  const selectedProject = ref<Project | null>(null);
  const selectedSubProjectId = ref<string>('');
  const selectedSubProject = ref<SubProject | null>(null);
  const selectedIntegrationId = ref<string | null>(null);
  const integrationStatusCount = ref<Record<string, number>>({});

  return {
    selectedProjectGroupId,
    selectedProjectGroup,
    selectedProjectId,
    selectedProject,
    selectedSubProjectId,
    selectedSubProject,
    selectedIntegrationId,
    integrationStatusCount,
  };
});
