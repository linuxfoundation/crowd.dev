<template>
  <div
    v-if="showBanner"
  >
    <div class="pt-14">
      <!-- Links to {sub-project} integrations page -->
      <banner
        v-if="integrationsWithErrors.length === 1"
        variant="alert"
      >
        <div
          class="flex flex-wrap items-center justify-center grow text-sm"
        >
          <span>Currently,</span>
          <span class="font-semibold mx-1">{{ integrationsWithErrors[0]?.name }}</span>
          <span>has integrations with connectivity issues</span>
          <router-link
            v-if="hasAccessToSegmentId(integrationsWithErrors[0].id)"
            :to="{
              name: 'integration',
              params: {
                id: integrationsWithErrors[0].id,
                grandparentId: selectedProjectGroup.id,
              },
            }"
          >
            <lf-button
              type="primary"
              size="small"
              class="ml-3"
            >
              Manage integrations
            </lf-button>
          </router-link>
        </div>
      </banner>

      <!-- Links to {project group} settings page -->
      <banner
        v-else-if="integrationsWithErrors.length > 1"
        variant="alert"
      >
        <div
          class="flex flex-wrap items-center justify-center grow text-sm"
        >
          <span>Currently,</span>
          <span class="font-semibold mx-1">several sub-projects</span>
          <span>have integrations with connectivity issues</span>
          <router-link
            :to="{
              name: 'adminProjects',
              params: {
                id: selectedProjectGroup.id,
              },
            }"
          >
            <lf-button
              type="primary"
              size="small"
              class="ml-3"
            >
              Project group settings
            </lf-button>
          </router-link>
        </div>
      </banner>

      <!-- Links to {sub-project} integrations page -->
      <banner
        v-else-if="integrationsWithNoData.length === 1"
        variant="alert"
      >
        <div
          class="flex flex-wrap items-center justify-center grow text-sm"
        >
          <span>Currently,</span>
          <span class="font-semibold mx-1">{{ integrationsWithNoData[0]?.name }}</span>
          <span>has integrations that are not receiving activities</span>
          <router-link
            v-if="hasAccessToSegmentId(integrationsWithNoData[0].id)"
            :to="{
              name: 'integration',
              params: {
                id: integrationsWithNoData[0].id,
                grandparentId: selectedProjectGroup.id,
              },
            }"
          >
            <lf-button
              type="primary"
              size="small"
              class="ml-3"
            >
              Manage integrations
            </lf-button>
          </router-link>
        </div>
      </banner>

      <!-- Links to {project group} settings page -->
      <banner
        v-else-if="integrationsWithNoData.length > 1"
        variant="alert"
      >
        <div
          class="flex flex-wrap items-center justify-center grow text-sm"
        >
          <span>Currently,</span>
          <span class="font-semibold mx-1">several sub-projects</span>
          <span>have integrations that are not receiving activities</span>
          <router-link
            :to="{
              name: 'adminProjects',
              params: {
                id: selectedProjectGroup.id,
              },
            }"
          >
            <lf-button
              type="primary"
              size="small"
              class="ml-3"
            >
              Project group settings
            </lf-button>
          </router-link>
        </div>
      </banner>
      <!-- TODO: Remove this banner once Confluence integrations is back up -->
      <banner
        variant="alert"
      >
        <div
          class="flex flex-wrap items-center justify-center grow text-sm py-2"
        >
          <span class="font-semibold">Temporary Disruption of Confluence Integration</span>
          <span>&nbsp;Confluence integration is currently stopped.
            The team is actively working on bringing the integration back and restore full functionality.</span>
        </div>
      </banner>
    </div>
  </div>
</template>

<script setup>
import Banner from '@/shared/banner/banner.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import {
  watch, ref, computed, onUnmounted,
} from 'vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import { getSegmentsFromProjectGroup } from '@/utils/segments';
import { isCurrentDateAfterGivenWorkingDays } from '@/utils/date';
import { useRoute } from 'vue-router';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import LfButton from '@/ui-kit/button/Button.vue';

const ERROR_BANNER_WORKING_DAYS_DISPLAY = 3;

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
const integrations = ref([]);
const fetchIntegrationTimer = ref(null);
const loading = ref(true);
const subProjects = ref([]);

const route = useRoute();

const { hasAccessToSegmentId } = usePermissions();

const integrationsWithErrors = computed(() => integrations.value.reduce((acc, integration) => {
  if (integration.status === 'error' && isCurrentDateAfterGivenWorkingDays(integration.updatedAt, ERROR_BANNER_WORKING_DAYS_DISPLAY)) {
    const hasSubProject = acc.some((sp) => sp.id === integration.segmentId);

    if (!hasSubProject) {
      const subproject = subProjects.value.find((sp) => sp.id === integration.segmentId);

      if (subproject) {
        acc.push(subproject);
      }
    }
  }

  return acc;
}, []));

const integrationsWithNoData = computed(() => integrations.value.reduce((acc, integration) => {
  if (integration.status === 'no-data') {
    const hasSubProject = acc.some((sp) => sp.id === integration.segmentId);

    if (!hasSubProject) {
      const subproject = subProjects.value.find((sp) => sp.id === integration.segmentId);

      if (subproject) {
        acc.push(subproject);
      }
    }
  }

  return acc;
}, []));

const integrationsInProgress = computed(() => integrations.value.reduce((acc, integration) => {
  if (integration.status === 'in-progress') {
    acc.integrations.push(integration);

    const hasSubProject = acc.subProjects.some((sp) => sp.id === integration.segmentId);

    if (!hasSubProject) {
      const subproject = subProjects.value.find((sp) => sp.id === integration.segmentId);

      if (subproject) {
        acc.subProjects.push(subproject);
      }
    }
  }

  return acc;
}, {
  integrations: [],
  subProjects: [],
}));

const showBanner = computed(() => (integrationsWithErrors.value.length
  || integrationsWithNoData.value.length) && !route.meta.hideBanner && !!selectedProjectGroup.value && !loading.value);

const fetchIntegrations = (projectGroup) => {
  if (projectGroup) {
    IntegrationService.list(null, null, null, null, getSegmentsFromProjectGroup(projectGroup))
      .then((response) => {
        integrations.value = response.rows;
      })
      .finally(() => {
        loading.value = false;
      });
  }
};

const startTimer = () => {
  fetchIntegrationTimer.value = setInterval(() => fetchIntegrations(selectedProjectGroup.value), 120000); // Fetch integrations every 30 seconds
};

// Stop the timer
const stopTimer = () => {
  clearInterval(fetchIntegrationTimer.value);
};

watch(selectedProjectGroup, (updatedProjectGroup, previousProjectGroup) => {
  stopTimer();

  if (previousProjectGroup?.id !== updatedProjectGroup?.id) {
    loading.value = true;
    fetchIntegrations(updatedProjectGroup);
  }

  if (!updatedProjectGroup) {
    subProjects.value = [];
  } else {
    subProjects.value = updatedProjectGroup.projects
      .reduce((acc, project) => {
        project.subprojects.forEach((subproject) => {
          if (subproject) {
            acc.push(subproject);
          }
        });

        return acc;
      }, []);
  }
}, {
  deep: true,
  immediate: true,
});

watch(
  integrationsInProgress,
  (newIntegrationsInProgress) => {
    stopTimer();

    if (newIntegrationsInProgress.integrations.length > 0) {
      startTimer();
    }
  },

  {
    deep: true,
  },
);

onUnmounted(() => {
  stopTimer();
});

</script>

<script>
export default {
  name: 'AppLfBanners',
};
</script>
