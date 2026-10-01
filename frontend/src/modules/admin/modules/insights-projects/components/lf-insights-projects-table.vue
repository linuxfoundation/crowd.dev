<template>
  <lf-table class="!overflow-visible" show-hover>
    <thead>
      <tr>
        <lf-table-head class="pl-2 w-[40%]">
          Project
        </lf-table-head>
        <lf-table-head class="pl-3 w-[26%]">
          Collection(s)
        </lf-table-head>
        <lf-table-head class="pl-3 min-w-[27%]">
          Associated company
        </lf-table-head>
        <lf-table-head class="pl-3 min-w-[15%]">
          Enabled
        </lf-table-head>
        <lf-table-head class="w-[7%]" />
      </tr>
    </thead>
    <tbody>
      <tr v-for="project of projects" :key="project.id">
        <lf-table-cell class="pl-2">
          <div class="flex items-center">
            <div class="inline-flex overflow-wrap items-center">
              <lf-avatar
                :src="project.logoUrl"
                :name="project.name"
                :size="24"
                class="!rounded-md border border-gray-200 mr-3 min-w-5"
              />
              <span
                class="text-black text-sm font-semibold line-clamp-2 w-auto"
              >
                {{ project.name }}
              </span>
            </div>
          </div>
        </lf-table-cell>

        <lf-table-cell class="pl-2">
          <app-lf-project-column
            :icon="'rectangle-history'"
            :title="'collections'"
            :projects="project.collections"
          />
        </lf-table-cell>

        <lf-table-cell class="pl-2">
          <div
            v-if="project.organization"
            class="border border-gray-200 rounded-[100px] w-fit px-2 py-1 bg-white flex items-center"
          >
            <lf-avatar
              :src="project.organization.logo"
              :name="project.organization.displayName"
              :size="14"
              class="!rounded-md border border-gray-200"
            />
            <span class="ml-2 text-gray-900 text-xs">
              {{ project.organization.displayName }}
            </span>
          </div>
          <span v-else class="text-gray-500 text-sm">No Company</span>
        </lf-table-cell>

        <lf-table-cell class="pl-3">
          <lf-switch
            :model-value="project.enabled"
            size="small"
            @update:model-value="
              changeProjectEnabled(project.id, $event as boolean)
            "
          />
        </lf-table-cell>

        <lf-table-cell class="pr-2">
          <lf-insights-project-dropdown
            :id="project.id"
            class="flex justify-end"
            @on-edit-project="emit('onEditProject', project.id)"
            @on-delete-project="emit('onDeleteProject', project.id)"
          />
        </lf-table-cell>
      </tr>
    </tbody>
  </lf-table>
</template>

<script setup lang="ts">
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableCell from '@/ui-kit/table/TableCell.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import AppLfProjectColumn from '@/shared/project-column/lf-project-column.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';
import { INSIGHTS_PROJECTS_SERVICE } from '@/modules/admin/modules/insights-projects/services/insights-projects.service';
import { useMutation, useQueryClient } from '@tanstack/vue-query';
import { TanstackKey } from '@/shared/types/tanstack';

import { ToastStore } from '@/shared/message/notification';
import LfInsightsProjectDropdown from './lf-insights-projects-dropdown.vue';
import type {
  InsightsProjectModel,
  InsightsProjectRequest,
} from '../models/insights-project.model';

defineProps<{
  projects: InsightsProjectModel[];
}>();

const emit = defineEmits<{(e: 'onEditProject', id: string): void;
  (e: 'onDeleteProject', id: string): void;
}>();

const queryClient = useQueryClient();
const onSuccess = (res: InsightsProjectModel) => {
  queryClient.invalidateQueries({
    queryKey: [TanstackKey.ADMIN_INSIGHTS_PROJECTS],
  });
  ToastStore.closeAll();
  ToastStore.success('Insights project updated successfully');
};

const onError = () => {
  ToastStore.closeAll();
  ToastStore.error('Something went wrong while updating the project');
};

const updateMutation = useMutation({
  mutationFn: ({ id, form }: { id: string; form: InsightsProjectRequest }) => INSIGHTS_PROJECTS_SERVICE.update(id, form),
  onSuccess,
  onError,
});

const changeProjectEnabled = (projectId: string, enabled: boolean) => {
  updateMutation.mutate({ id: projectId, form: { enabled } });
};
</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectsTable',
};
</script>
