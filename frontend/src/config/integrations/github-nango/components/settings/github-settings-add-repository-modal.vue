<template>
  <lf-modal
    v-model="isModalOpen"
    :close-function="() => !isOrganizationRepoLoading"
  >
    <div class="pb-6 px-6 h-[55vh]">
      <div class="sticky pt-5 bg-white z-10 top-0">
        <div class="flex justify-between items-center pb-5">
          <h5>Add repositories</h5>
          <lf-button
            type="secondary-ghost"
            icon-only
            @click="!isOrganizationRepoLoading ? (isModalOpen = false) : null"
          >
            <lf-icon name="xmark" />
          </lf-button>
        </div>
        <div class="pb-4">
          <lf-tabs v-model="tab" class="!w-full" :fragment="false">
            <lf-tab name="repositories" class="flex-grow">
              Repositories
            </lf-tab>
            <lf-tab name="organizations" class="flex-grow">
              Organizations
            </lf-tab>
          </lf-tabs>
        </div>

        <lf-search
          v-model="search"
          :lazy="true"
          class="!h-9"
          :placeholder="
            tab === 'repositories'
              ? 'Search GitHub repositories...'
              : 'Search GitHub organizations...'
          "
        />
      </div>

      <div
        class="flex flex-col py-4 max-h-[75%] h-full overflow-y-auto github-infinite-scroll"
      >
        <!-- Loading and empty search state -->
        <div
          v-if="!debouncedSearch || (loading && page === 0)"
          class="flex flex-col items-center justify-around grow pb-6"
        >
          <div class="py-4">
            <img
              :src="githubSearchImage"
              alt="GitHub Search"
              class="w-full max-w-100"
              :class="{ 'animate-pulse': loading }"
            />
          </div>
          <p v-if="loading" class="text-medium text-gray-500">
            <span v-if="tab === 'repositories'">Searching for repositories...</span>
            <span v-else>Searching for organizations...</span>
          </p>
          <p v-else class="font-semibold text-medium">
            Explore the entire GitHub database and sync any repository
          </p>
        </div>
        <div v-else>
          <!-- Repositories -->
          <div v-if="tab === 'repositories'" class="flex flex-col gap-5 mt-6">
            <!-- Repository list item -->
            <article
              v-for="repo of resultRepositories"
              :key="repo.url"
              class="flex justify-between items-center"
            >
              <div>
                <div class="flex items-center gap-1.5 mb-0.5">
                  <lf-svg name="git-repository" class="w-4 h-4 text-gray-900" />

                  <p class="text-small font-semibold">
                    {{ repo.name }}
                  </p>
                </div>
                <p class="text-tiny text-gray-500">
                  {{ repo.org?.name }}
                </p>
              </div>
              <div>
                <lf-button
                  v-if="isRepositoryAdded(repo)"
                  type="primary-ghost"
                  size="small"
                  class="!bg-primary-50 hover:!bg-primary-100"
                  @click="removeRepository(repo)"
                >
                  <lf-icon name="check" type="regular" />
                  <span>Added</span>
                </lf-button>
                <lf-button
                  v-else
                  type="primary-ghost"
                  size="small"
                  @click="addRepository(repo)"
                >
                  Add repository
                </lf-button>
              </div>
            </article>
            <div
              v-if="resultRepositories.length === 0"
              class="flex justify-center"
            >
              <div class="pt-12 flex flex-col items-center w-full max-w-100">
                <lf-svg
                  name="git-repository"
                  class="w-16 h-16 text-gray-300 mb-6"
                />
                <h6 class="text-center pb-3">
                  No repositories found
                </h6>
              </div>
            </div>
            <p v-else-if="loading" class="text-medium text-gray-500">
              loading more repositories...
            </p>
          </div>

          <!-- Organizations -->
          <div v-else-if="tab === 'organizations'" class="pt-6">
            <div class="flex flex-col gap-5">
              <!-- Organization list item -->
              <article
                v-for="org of resultOrganizations"
                :key="org.url"
                class="flex justify-between items-center"
              >
                <div class="flex items-center gap-3">
                  <lf-avatar
                    :name="org.name"
                    :src="org.logo"
                    :size="32"
                    class="!rounded border border-gray-200"
                  >
                    <template #placeholder>
                      <div
                        class="w-full h-full bg-gray-50 flex items-center justify-center"
                      >
                        <lf-icon
                          name="house-building"
                          :size="12"
                          class="text-gray-400"
                        />
                      </div>
                    </template>
                  </lf-avatar>
                  <div>
                    <p class="text-small font-semibold mb-0.5">
                      {{ org.name }}
                    </p>
                  </div>
                </div>
                <div>
                  <lf-button
                    v-if="isOrganizationSynced(org)"
                    type="primary-ghost"
                    size="small"
                    class="!bg-primary-50 hover:!bg-primary-100"
                    @click="removeOrganizations(org)"
                  >
                    <lf-icon name="check" type="regular" />
                    <span>Synced</span>
                  </lf-button>
                  <lf-button
                    v-else
                    type="primary-ghost"
                    size="small"
                    @click="addOrganizations(org)"
                  >
                    Sync organization
                  </lf-button>
                </div>
              </article>
              <div
                v-if="resultOrganizations.length === 0"
                class="flex justify-center"
              >
                <div class="pt-12 flex flex-col items-center w-full max-w-100">
                  <lf-icon
                    name="building"
                    type="regular"
                    :size="64"
                    class="text-gray-300 mb-6"
                  />
                  <h6 class="text-center pb-3">
                    No Github organizations found
                  </h6>
                </div>
              </div>
              <p v-else-if="loading" class="text-medium text-gray-500">
                loading more organizations...
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  </lf-modal>
</template>

<script lang="ts" setup>
import {
  computed,
  nextTick,
  onBeforeUnmount,
  onMounted,
  ref,
  watch,
} from 'vue';
import { debouncedRef } from '@vueuse/core';
import LfModal from '@/ui-kit/modal/Modal.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfSearch from '@/ui-kit/search/Search.vue';
import LfTabs from '@/ui-kit/tabs/Tabs.vue';
import LfTab from '@/ui-kit/tabs/Tab.vue';
import LfSvg from '@/shared/svg/svg.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import type {
  GitHubOrganization,
  GitHubRepository,
  GitHubSettingsRepository,
} from '@/config/integrations/github-nango/types/GithubSettings';
import { GithubApiService } from '@/config/integrations/github-nango/services/github.api.service';
import { ToastStore } from '@/shared/message/notification';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  modelValue: boolean;
  organizations: GitHubOrganization[];
  repositories: GitHubSettingsRepository[];
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'update:organizations', value: GitHubOrganization[]): void;
  (e: 'update:repositories', value: GitHubSettingsRepository[]): void;
}>();

const search = ref('');
const loading = ref(false);
const tab = ref('repositories');
const isOrganizationRepoLoading = ref(false);
const page = ref(0);
const pageSize = 20; // Adjust as needed
const noMoreData = ref(false);
let scrollContainer: HTMLElement | null = null;

const isModalOpen = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emit('update:modelValue', val);
  },
});

const repositories = computed<GitHubSettingsRepository[]>({
  get() {
    return props.repositories;
  },
  set(value) {
    emit('update:repositories', value);
  },
});

const organizations = computed<GitHubOrganization[]>({
  get() {
    return props.organizations;
  },
  set(value) {
    emit('update:organizations', value);
  },
});

const isRepositoryAdded = (repo: GitHubSettingsRepository) => repositories.value.some((r: GitHubSettingsRepository) => r.url === repo.url);
const isOrganizationSynced = (org: GitHubOrganization) => organizations.value.some((o: GitHubOrganization) => o.url === org.url);

const addRepository = (repo: GitHubSettingsRepository) => {
  if (
    !repositories.value.some(
      (r: GitHubSettingsRepository) => repo.url === r.url,
    )
  ) {
    repositories.value.push({ ...repo, updatedAt: dateHelper().toISOString() });
  }
};

const addOrganizations = (org: GitHubOrganization) => {
  organizations.value.push({ ...org, updatedAt: dateHelper().toISOString() });
  isOrganizationRepoLoading.value = true;
  GithubApiService.getOrganizationRepositories(org.name)
    .then((res) => {
      const newRepositories = (res as GitHubSettingsRepository[])
        .filter(
          (r: GitHubSettingsRepository) => !repositories.value.some(
            (repo: GitHubSettingsRepository) => repo.url === r.url,
          ),
        )
        .map((r: GitHubSettingsRepository) => ({
          ...r,
          org,
          updatedAt: dateHelper().toISOString(),
        }));
      repositories.value = [...repositories.value, ...newRepositories];
    })
    .finally(() => {
      isOrganizationRepoLoading.value = false;
    });
};

const removeRepository = (repo: any) => {
  repositories.value = repositories.value.filter(
    (r: any) => r.url !== repo.url,
  );
};
const removeOrganizations = (org: any) => {
  organizations.value = organizations.value.filter(
    (o: any) => o.url !== org.url,
  );
};

const resultRepositories = ref<GitHubRepository[]>([]);

const resultOrganizations = ref<GitHubOrganization[]>([]);

const searchForResults = () => {
  loading.value = true;
  (tab.value === 'repositories'
    ? GithubApiService.searchRepositories
    : GithubApiService.searchOrganizations)(search.value, page.value)
    .then((res) => {
      if (tab.value === 'repositories') {
        resultRepositories.value = [
          ...resultRepositories.value,
          ...res.rows,
        ] as GitHubRepository[];
        noMoreData.value = resultRepositories.value.length >= +res.count;
      } else {
        resultOrganizations.value = [
          ...resultOrganizations.value,
          ...res.rows,
        ] as GitHubOrganization[];
        noMoreData.value = resultOrganizations.value.length >= +res.count;
      }
    })
    .catch(() => {
      ToastStore.error(`There has been error fetching ${tab.value}`);
    })
    .finally(() => {
      loading.value = false;
    });
};

const debouncedSearch = debouncedRef(search, 500);

watch(
  () => debouncedSearch.value,
  (value: string) => {
    if (value.length > 0) {
      page.value = 0;
      noMoreData.value = false;
      resultRepositories.value = [];
      resultOrganizations.value = [];
      searchForResults();
    }
  },
);

watch(
  () => tab.value,
  () => {
    search.value = '';
  },
);

// Infinite scroll handler
function onScroll(e: Event) {
  if (!scrollContainer) return;
  const threshold = 20;

  const target = e.target as HTMLElement;
  if (
    !loading.value
    && !noMoreData.value
    && target.scrollHeight - target.scrollTop - target.clientHeight < threshold
  ) {
    page.value += pageSize;
    searchForResults();
  }
}

onMounted(() => {
  nextTick(() => {
    scrollContainer = document.querySelector('.github-infinite-scroll');
    if (scrollContainer) {
      scrollContainer.addEventListener('scroll', onScroll);
    }
  });
});

onBeforeUnmount(() => {
  if (scrollContainer) {
    scrollContainer.removeEventListener('scroll', onScroll);
  }
});

const githubSearchImage = new URL(
  '@/assets/images/integrations/github-search.png',
  import.meta.url,
).href;
</script>

<script lang="ts">
export default {
  name: 'LfGithubSettingsAddRepositoryModal',
};
</script>
