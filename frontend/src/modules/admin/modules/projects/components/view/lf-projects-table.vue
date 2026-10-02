<template>
  <section class="border border-gray-200 rounded-md">
    <!-- Header -->
    <div class="px-4 py-3 flex items-center justify-between bg-gray-50 rounded-md">
      <div class="flex items-center gap-4">
        <lf-project-status-pill :status="props.project.status" class="w-20" />
        <p class="font-semibold text-medium">
          {{ props.project.name }}
        </p>

        <span v-if="!props.project.isLF" class="px-2 py-1 text-tiny text-gray-900 bg-gray-200 border border-gray-200 rounded-[100px]">
          Non-Linux Foundation
        </span>
      </div>
      <div class="flex items-center gap-2">
        <lf-button
          v-if="
            hasPermission(LfPermission.subProjectCreate)
              && hasAccessToSegmentId(project.id)
          "
          type="secondary-ghost-light"
          icon-only
          @click="emit('onAddSubProject', project)"
        >
          <lf-icon name="layer-plus" :size="20" type="regular" />
        </lf-button>
        <app-lf-projects-dropdown
          :id="project.id"
          @on-edit-project="emit('onEditProject', project.id)"
          @on-add-sub-project="emit('onAddSubProject', project)"
        />
      </div>
    </div>
    <!-- Subprojects -->
    <article
      v-for="subproject of props.project.subprojects"
      :key="subproject.id"
      class="border-t border-gray-200 px-4 py-3 flex items-center justify-between"
    >
      <div class="flex items-center gap-4">
        <lf-project-status-pill :status="subproject.status" class="w-20" />
        <p class="text-medium">
          {{ subproject.name }}
        </p>
        <router-link
          v-if="subproject.insightsProjectId && subproject.insightsProjectName && isTeamUser"
          :to="{
            name: 'adminPanel',
            query: {
              search: subproject.insightsProjectName,
            },
            hash: '#projects',
          }"
        >
          <span
            class="text-tiny font-semibold rounded-full px-2.5 text-center h-4
          bg-transparent text-gray-900 outline-1 outline-gray-200 outline truncate block max-w-[150px]"
          >
            Insights: {{ subproject.insightsProjectName }}
          </span>
        </router-link>
        <app-lf-project-integration-column
          :segment-id="subproject.id"
          :integrations="subproject.integrations"
          :progress="props.progress"
          :progress-error="progressError"
        />
      </div>
      <div class="flex items-center gap-2">
        <router-link
          :to="{
            name: 'integration',
            params: {
              id: subproject.id,
              grandparentId: route.params.id,
            },
          }"
        >
          <lf-button type="primary-ghost">
            <lf-icon name="grid-round-2-plus" :size="16" type="regular" />
            Integrations
          </lf-button>
        </router-link>
        <app-lf-sub-projects-dropdown
          :id="subproject.id"
          @on-edit-sub-project="emit('onEditSubProject', subproject.id)"
        />
      </div>
    </article>
  </section>
</template>

<script setup>
import AppLfProjectsDropdown from '@/modules/admin/modules/projects/components/lf-projects-dropdown.vue';
import AppLfSubProjectsDropdown from '@/modules/admin/modules/projects/components/lf-sub-projects-dropdown.vue';
import { useRoute } from 'vue-router';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfProjectStatusPill from '@/modules/admin/modules/projects/components/fragments/lf-status-pill.vue';
import { computed } from 'vue';
import config from '@/config';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import AppLfProjectIntegrationColumn from '../fragments/lf-project-integration-column.vue';

const props = defineProps({
  project: {
    type: Object,
    required: true,
  },
  progress: {
    type: Array,
    required: false,
    default: () => [],
  },
  progressError: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits([
  'onEditProject',
  'onEditSubProject',
  'onAddSubProject',
]);

const route = useRoute();

const { hasPermission, hasAccessToSegmentId } = usePermissions();

const authStore = useAuthStore();
const userId = computed(() => authStore.user?.id);
const teamUserIds = computed(() => config.permissions.teamUserIds);
const env = computed(() => config.env);

const isTeamUser = computed(() => env.value !== 'production' || teamUserIds.value?.includes(userId.value));

</script>

<script>
export default {
  name: 'AppLfProjectsTable',
};
</script>

<style lang="scss">
#projects-table {
  @apply rounded-lg shadow-sm border border-solid border-gray-200;

  thead .table-columns {
    @apply align-middle h-auto px-2 py-4;
    @apply normal-case bg-gray-50 #{!important};

    &:first-child {
      @apply pl-4;
    }

    &:last-child {
      @apply pr-3;
    }

    .cell {
      @apply px-0 #{!important};
    }
  }

  tbody {
    .table-columns {
      @apply align-middle px-2 py-4 border-b-0;

      &:first-child {
        @apply pl-4;
      }

      &:last-child {
        @apply pr-3;
      }

      &.el-table-fixed-column--right .cell {
        @apply justify-end;
      }
    }

    .cell {
      @apply px-0 #{!important};
    }
  }

  .el-table__empty-text {
    @apply w-full;
  }

  .el-table__append-wrapper {
    position: sticky !important;
    left: 0px !important;
    width: fit-content;
  }
}
</style>
