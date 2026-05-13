<template>
  <div class="mx-auto max-w-254">
    <div class="sticky -top-5 -mt-5 z-10 bg-white border-b border-b-white">
      <!-- Back button -->
      <div class="pt-6 flex items-center gap-1 text-xs">
        <router-link
          :to="{
            name: 'adminProjects',
            params: {
              id: grandparentId,
            },
          }"
        >
          {{ getSegmentName(grandparentId) || subproject?.grandparentName }}
        </router-link>
        <p v-if="subproject" class="text-gray-600">
          >
          {{ subproject?.name }}
        </p>
        <p v-if="subproject" class="text-gray-600">
          (Integrations)
        </p>
      </div>

      <!-- Header -->
      <div class="py-5">
        <h4 class="pb-1">
          Integrations
        </h4>
        <p class="text-small text-gray-500">
          Connect with the data sources where interactions happen within your
          community.
          <a
            aria-label="Question"
            class="btn btn-link btn-link--primary hover:no-underline gap-1 "
            :href="links.integrations"
            target="_blank"
            rel="noopener noreferrer"
          >
            <lf-icon name="book-open" :size="16" />
            Learn more
          </a>
        </p>
      </div>

      <!-- Tabs -->
      <section>
        <div class="w-full bg-white mb-8">
          <lfx-tabs v-model="tab">
            <lfx-tab v-model="tab" name="all">
              All integrations
            </lfx-tab>
            <lfx-tab
              v-for="(status, key) in lfIntegrationStatusesTabs"
              :key="key"
              v-model="tab"
              :name="key"
            >
              <div class="flex items-center gap-1.5">
                <lf-icon
                  v-if="status.chipStatus?.icon || status.status.icon"
                  :name="status.chipStatus?.icon || status.status.icon"
                  :type="tab === key ? 'solid' : 'light'"
                  class="mt-0.5"
                  :class="tab === key ? status.chipStatus?.color || status.status.color : 'text-gray-500'"
                  :size="16"
                />
                <span>{{ status.tabs.text }}</span>
              </div>
            </lfx-tab>
          </lfx-tabs>
        </div>
      </section>
    </div>

    <section v-loading="loadingFetch">
      <app-integration-progress-wrapper
        v-if="!loadingFetch"
        :segments="[route.params.id as string]"
      >
        <template #default="{ progress, progressError }">
          <div v-if="platformsByStatus.length > 0" class="flex flex-col gap-6">
            <lf-integration-list-item
              v-for="key in platformsByStatus"
              :key="key"
              :config="lfIntegrations(useGitHubNango)[key]"
              :progress="progress"
              :progress-error="progressError"
            />
          </div>
          <div v-else class="pt-12 flex flex-col items-center">
            <lf-icon name="grid-round" :size="120" class="text-gray-300" />
            <h6 class="text-center pt-6">
              {{
                lfIntegrationStatusesTabs[tab]?.tabs?.empty
                  || 'No integrations connected'
              }}
            </h6>
          </div>
        </template>
      </app-integration-progress-wrapper>
    </section>
  </div>
</template>

<script lang="ts" setup>
import { useRoute } from 'vue-router';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { computed, onMounted, ref } from 'vue';
import { getSegmentName } from '@/utils/segments';
import { lfIntegrationStatusesTabs } from '@/modules/admin/modules/integration/config/status';
import { lfIntegrations } from '@/config/integrations';
import LfxTabs from '@/ui-kit/lfx/tabs/tabs.vue';
import LfIntegrationListItem from '@/modules/admin/modules/integration/components/integration-list-item.vue';
import LfxTab from '@/ui-kit/lfx/tabs/tab.vue';
import AppIntegrationProgressWrapper from '@/modules/integration/components/integration-progress-wrapper.vue';
import { mapActions, mapGetters } from '@/shared/vuex/vuex.helpers';
import { useAuthStore } from '@/modules/auth/store/auth.store';
import config from '@/config';
import { links } from '@/config/links';

const route = useRoute();

const { findSubProject } = useLfSegmentsStore();
const { doFetch } = mapActions('integration');
const { array, loadingFetch } = mapGetters('integration');

const { id, grandparentId } = route.params;

const authStore = useAuthStore();
const userId = computed(() => authStore.user?.id);
const teamUserIds = computed(() => config.permissions.teamUserIds);
const env = computed(() => config.env);

const isTeamUser = computed(() => env.value !== 'production' || teamUserIds.value?.includes(userId.value));

const useGitHubNango = computed(() => {
  const githubIntegration = array.value.find(
    (integration: any) => integration.platform === 'github',
  );
  if (githubIntegration) {
    return !!githubIntegration.isNango;
  }
  return !!isTeamUser.value;
});

const subproject = ref<any>();

const tab = ref('all');

const platformsByStatus = computed(() => {
  const statusConfig = lfIntegrationStatusesTabs[tab.value];
  const all = Object.keys(lfIntegrations(useGitHubNango.value));
  if (!statusConfig) {
    return all;
  }
  if (statusConfig.key === 'notConnected') {
    return all.filter(
      (platform) => !array.value
        .map((integration: any) => integration.platform)
        .includes(platform),
    );
  }
  const matching = array.value
    .filter((integration: any) => statusConfig.show(integration))
    .map((integration: any) => integration.platform);
  return all.filter((platform) => matching.includes(platform));
});

onMounted(() => {
  localStorage.setItem('segmentId', id as string);
  localStorage.setItem('segmentGrandparentId', grandparentId as string);

  doFetch();
  findSubProject(id).then((res) => {
    subproject.value = res;
  });
});
</script>

<script lang="ts">
export default {
  name: 'LfIntegrationListPage',
};
</script>
