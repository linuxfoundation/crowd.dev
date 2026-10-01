<template>
  <div>
    <div v-if="cForm.projects.length > 0">
      <lf-button
        class="w-fit mb-2"
        size="medium"
        type="primary-ghost"
        @click="onCreateProject"
      >
        <lf-icon name="plus" :size="16" />
        Add a new project
      </lf-button>
      <!-- Search input -->
      <div class="w-full relative">
        <lf-insights-projects-list-dropdown
          :selected-projects="cForm.projects"
          @on-add-project="onAddProject"
        />
      </div>

      <lf-table class="!overflow-visible" show-hover>
        <thead>
          <tr>
            <lf-table-head class="pl-2 min-w-[45%]" />
            <lf-table-head class="pl-3 w-12" />
          </tr>
        </thead>
        <tbody>
          <tr v-for="project of cForm.projects" :key="project.id">
            <lf-table-cell class="pl-2">
              <div class="flex items-center">
                <div class="inline-flex flex-wrap overflow-wrap items-center">
                  <lf-avatar
                    :src="project.logoUrl"
                    :name="project.name"
                    :size="24"
                    class="!rounded-md border border-gray-200 mr-3"
                  />
                  <span
                    class="text-black text-sm font-semibold line-clamp-2 w-auto"
                  >
                    {{ project.name }}
                  </span>
                  <span
                    v-if="project.starred"
                    class="ml-3 text-primary-800 text-2xs bg-primary-50 border border-primary-200 rounded-md px-1"
                  >
                    Featured
                  </span>
                </div>
              </div>
            </lf-table-cell>

            <lf-table-cell class="pr-2 flex justify-end">
              <lf-collection-add-dropdown
                :id="project.id"
                :starred="project.starred"
                @on-edit-project="onEditProject"
                @on-featured-project="onFeaturedProject"
                @on-remove-project="onRemoveProject"
              />
            </lf-table-cell>
          </tr>
        </tbody>
      </lf-table>
    </div>

    <div v-else class="flex flex-col items-center">
      <app-empty-state-cta
        class="w-full !pb-0"
        icon="rectangle-history"
        title="No projects yet"
        description="Start adding projects into this collection"
      />
      <lf-button
        class="w-fit mb-2"
        size="medium"
        type="primary-ghost"
        @click="onCreateProject"
      >
        <lf-icon name="plus" :size="16" />
        Add a new project
      </lf-button>
      <!-- Search input -->
      <div class="w-3/5 relative">
        <lf-insights-projects-list-dropdown
          :selected-projects="cForm.projects"
          @on-add-project="onAddProject"
        />
      </div>
    </div>
  </div>

  <lf-insights-project-add
    v-if="isProjectDialogOpen"
    v-model="isProjectDialogOpen"
    :insights-project-id="projectEditObject?.id"
    :display-back-button="true"
    @on-insights-project-created="onInsightsProjectDialogCloseSuccess"
    @on-insights-project-edited="onInsightsProjectDialogCloseSuccess"
    @update:model-value="onInsightsProjectDialogClose"
  />
</template>

<script setup lang="ts">
import { reactive, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import AppEmptyStateCta from '@/shared/empty-state/empty-state-cta.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import LfTableCell from '@/ui-kit/table/TableCell.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import { cloneDeep, sortBy } from 'lodash';
import LfInsightsProjectsListDropdown from './lf-insights-projects-list-dropdown.vue';
import LfCollectionAddDropdown from './lf-collection-add-dropdown.vue';
import { useInsightsProjectsStore } from '../../insights-projects/pinia';
import type { CollectionFormModel } from '../models/collection.model';
import LfInsightsProjectAdd from '../../insights-projects/components/lf-insights-project-add.vue';
import type { InsightsProjectModel } from '../../insights-projects/models/insights-project.model';

const insightsProjectsStore = useInsightsProjectsStore();

const props = defineProps<{
  form: CollectionFormModel;
}>();

const cForm = reactive<CollectionFormModel>(props.form);
const isProjectDialogOpen = ref(false);
const projectEditObject = ref<InsightsProjectModel | undefined>(undefined);

const onFeaturedProject = (projectId: string) => {
  const project = cForm.projects.find((project) => project.id === projectId);
  if (project) {
    project.starred = !project.starred;
  }
};

const onRemoveProject = (projectId: string) => {
  cForm.projects = cForm.projects.filter((project) => project.id !== projectId);
};

const onAddProject = (projectId: string) => {
  const project = insightsProjectsStore.getInsightsProject(projectId);
  if (project) {
    cForm.projects.push(project);
    cForm.projects = sortBy(cForm.projects, 'name');
  }
};

const onCreateProject = () => {
  isProjectDialogOpen.value = true;
};

const onEditProject = (projectId: string) => {
  isProjectDialogOpen.value = true;
  projectEditObject.value = cloneDeep(
    cForm.projects.find((project) => project.id === projectId),
  );
};

const onInsightsProjectDialogCloseSuccess = (project: InsightsProjectModel) => {
  isProjectDialogOpen.value = false;
  if (projectEditObject.value) {
    const projectIndex = cForm.projects.findIndex(
      (project) => project.id === projectEditObject.value!.id,
    );
    if (projectIndex !== -1) {
      cForm.projects[projectIndex] = project;
      insightsProjectsStore.updateInsightsProject(project);
    }
  } else {
    insightsProjectsStore.createInsightsProject(project);
    cForm.projects.push(project);
    cForm.projects = sortBy(cForm.projects, 'name');
  }
  projectEditObject.value = undefined;
};

const onInsightsProjectDialogClose = () => {
  isProjectDialogOpen.value = false;
  projectEditObject.value = undefined;
};
</script>

<script lang="ts">
export default {
  name: 'LfCollectionAddProjectsTab',
};
</script>
