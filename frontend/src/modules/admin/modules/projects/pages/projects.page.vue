<template>
  <app-page-wrapper>
    <router-link
      v-if="hasPermission(LfPermission.projectGroupEdit)"
      :to="{
        name: 'adminPanel',
        query: {
          activeTab: 'project-groups',
        },
      }"
    >
      <lf-button type="secondary-ghost" class="mb-6">
        <lf-icon name="angle-left" type="regular" />
        <span>Project groups</span>
      </lf-button>
    </router-link>

    <div class="w-full flex items-center justify-between mb-6 pt-7 border-t border-gray-100">
      <div class="flex items-center gap-4 whitespace-nowrap">
        <h4 class="text-gray-900 flex items-center">
          {{ projectGroupForm.name }}
        </h4>
        <app-lf-status-pill :status="projectGroupForm.status" />
        <app-lf-project-count :count="projectCount" />
      </div>

      <div class="flex items-center gap-4">
        <lf-button
          v-if="pagination.total && hasPermission(LfPermission.projectCreate) && hasAccessToSegmentId(route.params.id)"
          size="medium"
          type="secondary-ghost"
          @click="onAddProject"
        >
          <lf-icon name="layer-plus" type="regular" />
          Add project
        </lf-button>

        <app-lf-project-groups-dropdown
          v-if="hasPermission(LfPermission.projectGroupCreate)"
          :id="projectGroupForm.id"
          show-edit-only
          @on-edit-project-group="onEditProjectGroup"
        />
      </div>
    </div>

    <!-- Search input -->
    <app-lf-search-input
      v-if="pagination.total"
      placeholder="Search projects..."
      @on-change="onSearchProjects"
    />

    <div
      v-if="loading"
      v-loading="loading"
      class="app-page-spinner h-16 !relative !min-h-5 mt-10"
    />
    <div v-else>
      <!-- Empty state -->
      <app-empty-state-cta
        v-if="!pagination.total"
        class="mt-20"
        icon="layer-group"
        title="No projects yet"
        :description="`${!(hasPermission(LfPermission.projectCreate) && hasAccessToSegmentId(route.params.id))
          ? 'Ask an administrator to a' : 'A'}dd your first project and start collecting data from your community`"
        :cta-btn="hasPermission(LfPermission.projectCreate) && hasAccessToSegmentId(route.params.id) ? 'Add project' : null"
        @cta-click="onAddProject"
      />

      <app-empty-state-cta
        v-else-if="!pagination.count"
        icon="layer-group"
        title="No projects found"
        description="We couldn't find any results that match your search criteria, please try a different query"
      />

      <div v-else class="mt-6 flex flex-col gap-6">
        <app-integration-progress-wrapper :segments="segmentIds">
          <template #default="{ progress, progressError }">
            <app-lf-projects-table
              v-for="project in projects.list"
              :key="project.id"
              :project="project"
              :progress="progress"
              :progress-error="progressError"
              @on-edit-project="onEditProject"
              @on-edit-sub-project="onEditSubProject"
              @on-add-sub-project="onAddSubProject"
            />
          </template>
        </app-integration-progress-wrapper>

        <div v-if="!!pagination.count && !loading">
          <app-infinite-pagination
            :total="pagination.count"
            :page-size="Number(pagination.pageSize)"
            :current-page="pagination.currentPage || 1"
            :is-loading="projects.paginating"
            use-slot
            @load-more="onLoadMore"
          >
            <div
              class="pt-10 pb-6 gap-4 flex justify-center items-center"
            >
              <p class="text-small text-gray-400">
                {{ projects.list.length }} of {{ pagination.total }} projects
              </p>
              <lf-button
                type="primary-ghost"
                loading-text="Loading projects..."
                :loading="projects.paginating"
                @click="onLoadMore(pagination.currentPage + 1)"
              >
                Load more
              </lf-button>
            </div>
          </app-infinite-pagination>
        </div>
      </div>
    </div>

    <app-lf-project-form
      v-if="isProjectFormDrawerOpen"
      :id="projectForm.id"
      v-model="isProjectFormDrawerOpen"
      :parent-slug="projectGroupForm.slug"
      :is-l-f-project="projectGroupForm.isLF"
      @on-success="onProjectGroupEdited"
    />

    <app-lf-sub-project-form
      v-if="isSubProjectFormDrawerOpen"
      :id="subProjectForm.id"
      v-model="isSubProjectFormDrawerOpen"
      :is-l-f-project="projectGroupForm.isLF"
      :parent-slug="projectForm.slug"
      :parent-id="projectForm.id"
      :grandparent-slug="projectGroupForm.slug"
      :grandparent-id="projectGroupForm.id"
      @on-success="onProjectGroupEdited"
    />

    <app-lf-project-group-form
      v-if="isProjectGroupFormDrawerOpen"
      :id="projectGroupForm.id"
      v-model="isProjectGroupFormDrawerOpen"
      @on-project-group-edited="onProjectGroupEdited"
    />
  </app-page-wrapper>
</template>

<script setup>
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { useRoute } from 'vue-router';
import {
  computed, onMounted, reactive, ref,
} from 'vue';
import AppLfProjectGroupForm from '@/modules/admin/modules/projects/components/form/lf-project-group-form.vue';
import AppLfProjectForm from '@/modules/admin/modules/projects/components/form/lf-project-form.vue';
import AppLfSubProjectForm from '@/modules/admin/modules/projects/components/form/lf-sub-project-form.vue';
import AppLfProjectsTable from '@/modules/admin/modules/projects/components/view/lf-projects-table.vue';
import AppLfSearchInput from '@/modules/admin/modules/projects/components/view/lf-search-input.vue';
import { storeToRefs } from 'pinia';
import AppIntegrationProgressWrapper from '@/modules/integration/components/integration-progress-wrapper.vue';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import AppLfProjectGroupsDropdown from '@/modules/admin/modules/projects/components/lf-project-groups-dropdown.vue';
import AppLfStatusPill from '@/modules/admin/modules/projects/components/fragments/lf-status-pill.vue';
import AppLfProjectCount from '@/shared/project-column/lf-project-count.vue';

const route = useRoute();
const lsSegmentsStore = useLfSegmentsStore();
const { projects } = storeToRefs(lsSegmentsStore);
const {
  findProjectGroup, searchProject, listProjects, doChangeProjectCurrentPage,
} = lsSegmentsStore;

const { hasPermission, hasAccessToSegmentId } = usePermissions();
const { trackEvent } = useProductTracking();

const loadingProjectGroup = ref(true);
const projectGroupForm = reactive({
  slug: null,
  name: null,
  isLF: true,
});
const projectForm = reactive({
  id: null,
  slug: null,
});
const subProjectForm = reactive({
  id: null,
});
const isProjectGroupFormDrawerOpen = ref(false);
const isProjectFormDrawerOpen = ref(false);
const isSubProjectFormDrawerOpen = ref(false);

const loading = computed(() => projects.value.loading || loadingProjectGroup.value);
const pagination = computed(() => projects.value.pagination);
const searchQuery = ref('');

const segmentIds = computed(() => projects.value.list.map((p) => p.subprojects.map((sp) => sp.id)).flat() || []);
const projectCount = computed(() => pagination.value.total);

onMounted(() => {
  loadProjectGroups();
});

const onProjectGroupEdited = () => {
  loadProjectGroups();
};

const loadProjectGroups = () => {
  findProjectGroup(route.params.id)
    .then((response) => {
      Object.assign(projectGroupForm, response);

      listProjects({ parentSlug: projectGroupForm.slug, reset: true });
    }).finally(() => {
      loadingProjectGroup.value = false;
    });
};

const onLoadMore = () => {
  if (!projects.value.paginating) {
    if (searchQuery.value && searchQuery.value !== '') {
      searchProject(searchQuery.value, pagination.value.currentPage + 1);
    } else {
      doChangeProjectCurrentPage(pagination.value.currentPage + 1);
    }
  }
};

const onEditProjectGroup = () => {
  isProjectGroupFormDrawerOpen.value = true;
};

const onAddProject = () => {
  projectForm.id = null;
  isProjectFormDrawerOpen.value = true;
};

const onEditProject = (id) => {
  projectForm.id = id;
  isProjectFormDrawerOpen.value = true;
};

const onEditSubProject = (id, parentSlug) => {
  subProjectForm.id = id;
  projectForm.slug = parentSlug;
  isSubProjectFormDrawerOpen.value = true;
};

const onAddSubProject = ({ slug, id }) => {
  subProjectForm.id = null;
  projectForm.id = id;
  projectForm.slug = slug;
  isSubProjectFormDrawerOpen.value = true;
};

const onSearchProjects = (query) => {
  trackEvent({
    key: FeatureEventKey.SEARCH_PROJECTS,
    type: EventType.FEATURE,
  });

  searchQuery.value = query;
  searchProject(query);
};
</script>

<script>
export default {
  name: 'AppLfProjectsPage',
};
</script>
