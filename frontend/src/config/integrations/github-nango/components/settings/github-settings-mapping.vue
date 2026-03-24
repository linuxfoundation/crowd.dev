<template>
  <div>
    <div class="flex justify-between pb-6">
      <lf-button type="primary-link" class="!font-medium" @click="emit('add')">
        <lf-icon name="plus" />
        Sync repositories/organizations
      </lf-button>
      <lf-button type="secondary-link" class="!font-medium" @click="isBulkSelectOpened = true">
        <lf-icon name="list-check" type="light" class="!font-light" />
        Bulk mapping
      </lf-button>
    </div>
    <div class="mb-6">
      <lf-search
        v-model="search"
        placeholder="Search repositories..."
        class="!h-9 !rounded-full"
      />
    </div>
    <div v-if="filteredRepos.length === 0" class="flex justify-center">
      <div class="pt-12 flex flex-col items-center w-full max-w-100">
        <lf-svg name="git-repository" class="w-16 h-16 text-gray-300 mb-6" />
        <h6 class="text-center pb-3">
          No repositories
        </h6>
        <p class="text-center text-small text-gray-500 pb-6">
          No repositories matching your search criteria
        </p>
      </div>
    </div>
    <div v-else class="flex flex-col gap-6">
      <lf-github-settings-org-item
        v-for="o of allOrganizations"
        :key="o.url"
        v-model:organizations="orgs"
        v-model:repositories="repos"
        :organization="o"
        @remove-mapping="(urls) => urls.forEach((url) => removeMapping(url))"
      >
        <lf-table>
          <thead class="!border-b-gray-200">
            <tr>
              <th class="w-5/12 !h-10 !text-gray-500">
                Repository
              </th>
              <th class="w-7/12 !h-10 !text-gray-500">
                Mapped with
              </th>
            </tr>
          </thead>
          <tbody v-if="subprojects.length > 0">
            <lf-github-settings-repo-item
              v-for="repo of o.repositories"
              :key="repo.url"
              v-model:repositories="repos"
              v-model="repoMappings[repo.url]"
              :subprojects="subprojects"
              :repository="repo"
              :org-syncing="isOrgSyncing(o)"
              @remove-mapping="removeMapping"
            />
          </tbody>
        </lf-table>
      </lf-github-settings-org-item>
    </div>
  </div>
  <lf-github-settings-repositories-bulk-select
    v-model="isBulkSelectOpened"
    :repositories="repositories"
    :subprojects="subprojects"
    @apply="repoMappings = { ...repoMappings, ...$event }"
  />
</template>

<script lang="ts" setup>
import { computed, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfSearch from '@/ui-kit/search/Search.vue';
import LfGithubSettingsRepositoriesBulkSelect
  from '@/config/integrations/github-nango/components/settings/github-settings-repositories-bulk-select.vue';
import {
  GitHubOrganization,
  GitHubSettingsRepository,
} from '@/config/integrations/github-nango/types/GithubSettings';
import LfGithubSettingsOrgItem from '@/config/integrations/github-nango/components/settings/github-settings-org-item.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfGithubSettingsRepoItem from '@/config/integrations/github-nango/components/settings/github-settings-repo-item.vue';
import LfSvg from '@/shared/svg/svg.vue';

const props = defineProps<{
  repositories: GitHubSettingsRepository[];
  organizations: GitHubOrganization[];
  mappings: Record<string, string>;
  subprojects: any[];
}>();

const emit = defineEmits<{(e: 'update:repositories', value: GitHubSettingsRepository[]): void;
  (e: 'update:organizations', value: GitHubOrganization[]): void;
  (e: 'update:mappings', value: Record<string, string>): void;
  (e: 'add'): void;
}>();

const search = ref('');
const isBulkSelectOpened = ref(false);

const repos = computed<GitHubSettingsRepository[]>({
  get: () => props.repositories,
  set: (value) => emit('update:repositories', value),
});

const orgs = computed<GitHubOrganization[]>({
  get: () => props.organizations,
  set: (value) => emit('update:organizations', value),
});

const isOrgSyncing = (org: GitHubOrganization) => orgs.value.some((o) => o.url === org.url);

const repoMappings = computed<Record<string, string>>({
  get: () => props.mappings,
  set: (value) => emit('update:mappings', value),
});

const filteredRepos = computed(() => repos.value.filter(
  (r) => r.name.toLowerCase().includes(search.value.toLowerCase())
      || (r.org?.name || '').toLowerCase().includes(search.value.toLowerCase()),
));

const allOrganizations = computed<any[]>(() => {
  const owners = new Set();
  return filteredRepos.value.reduce((acc: any[], r) => {
    if (!owners.has(r.org!.name)) {
      owners.add(r.org!.name);
      acc.push({
        ...r.org!,
        repositories: props.repositories.filter(
          (repo) => repo.org!.url === r.org!.url,
        ),
      });
    }
    return acc;
  }, []);
});

const removeMapping = (repoUrl: string) => {
  const newMappings = { ...repoMappings.value };
  delete newMappings[repoUrl];
  repoMappings.value = newMappings;
};
</script>

<script lang="ts">
export default {
  name: 'LfGithubSettingsMapping',
};
</script>
