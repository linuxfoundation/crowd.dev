<template>
  <div class="relative not-clickable !px-0 !pt-0 !pb-6">
    <lf-table class="!overflow-visible" show-hover>
      <thead>
        <tr>
          <lf-table-head class="pl-2 w-20">
            Status
          </lf-table-head>
          <lf-table-head class="pl-3 min-w-35">
            Project group
          </lf-table-head>
          <lf-table-head>
            Projects
          </lf-table-head>
          <lf-table-head class="w-12" />
        </tr>
      </thead>
      <tbody>
        <tr v-for="projectGroup in pagination.rows" :key="projectGroup.id" class="cursor-pointer" @click="handleRowClick(projectGroup)">
          <lf-table-cell class="pl-2">
            <app-lf-status-pill :status="projectGroup.status" class="w-20" />
          </lf-table-cell>
          <lf-table-cell class="pl-3">
            <div class="text-sm font-semibold">
              {{ projectGroup.name }}
            </div>
          </lf-table-cell>
          <lf-table-cell>
            <app-lf-project-column
              :projects="projectGroup.projects || []"
              :project-count="projectGroup.projectCount"
            />
          </lf-table-cell>
          <lf-table-cell class="pr-2 flex justify-end">
            <app-lf-project-groups-dropdown
              :id="projectGroup.id"
              @on-edit-project-group="emit('onEditProjectGroup', projectGroup.id)"
              @on-add-project="emit('onAddProject', projectGroup.slug)"
            />
          </lf-table-cell>
        </tr>
      </tbody>
    </lf-table>

    <div v-if="!!pagination.count && !loading">
      <app-infinite-pagination
        :total="pagination.count"
        :page-size="pagination.limit"
        :current-page="(pagination.offset / pagination.limit) || 1"
        :is-loading="isFetchingNextPage"
        :use-slot="true"
        @load-more="onLoadMore"
      >
        <div
          class="pt-10 pb-6 gap-4 flex justify-center items-center"
        >
          <p class="text-small text-gray-400">
            {{ pagination.rows.length }} of {{ pagination.count }} project groups
          </p>
          <lf-button
            type="primary-ghost"
            loading-text="Loading project groups..."
            :loading="isFetchingNextPage"
            @click="onLoadMore()"
          >
            Load more
          </lf-button>
        </div>
      </app-infinite-pagination>
    </div>
  </div>
</template>

<script setup lang="ts">
import AppLfProjectGroupsDropdown from '@/modules/admin/modules/projects/components/lf-project-groups-dropdown.vue';
import { useRouter } from 'vue-router';
import LfButton from '@/ui-kit/button/Button.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableCell from '@/ui-kit/table/TableCell.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import AppLfProjectColumn from '@/shared/project-column/lf-project-column.vue';
import { ProjectGroup } from '@/modules/lf/segments/types/Segments';
import { Pagination } from '@/shared/types/Pagination';
import AppLfStatusPill from '../fragments/lf-status-pill.vue';

defineProps<{
  loading: boolean;
  isFetchingNextPage: boolean;
  pagination: Pagination<ProjectGroup>;
}>();

const emit = defineEmits<{(e: 'onEditProjectGroup', id: string): void;
  (e: 'onAddProject', slug: string): void;
  (e: 'onLoadMore'): void;
}>();
const router = useRouter();

const onLoadMore = () => {
  emit('onLoadMore');
};

const handleRowClick = (row: ProjectGroup) => {
  router.push({
    name: 'adminProjects',
    params: { id: row.id },
  });
};
</script>

<script lang="ts">
export default {
  name: 'AppLfProjectGroupsTable',
};
</script>
