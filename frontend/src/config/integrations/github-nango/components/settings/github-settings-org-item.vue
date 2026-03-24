<template>
  <div>
    <div
      class="bg-gray-50 px-2 py-1.5 border-t border-b border-gray-200 flex flex-col"
    >
      <div class="flex w-full items-center justify-between gap-3">
        <!-- Org information -->
        <div class="flex items-center gap-3">
          <lf-avatar
            :name="props.organization.name"
            :src="props.organization.logo"
            :size="24"
            class="!rounded border border-gray-200"
          >
            <template #placeholder>
              <div
                class="w-full h-full bg-gray-50 flex items-center justify-center"
              >
                <lf-icon name="house-building" :size="12" class="text-gray-400" />
              </div>
            </template>
          </lf-avatar>
          <p class="text-small font-semibold">
            {{ props.organization.name }}
          </p>
        </div>

        <div class="flex items-center gap-2">
          <lf-badge
            v-if="isSynced"
            type="primary"
            class="!flex items-center gap-1 !rounded-full !px-2"
          >
            <lf-icon name="arrows-rotate" />
            Synced
          </lf-badge>
          <lf-dropdown placement="bottom-end" width="11.125rem">
            <template #trigger>
              <lf-button type="secondary-ghost" icon-only>
                <lf-icon name="ellipsis" type="light" class="!font-light" />
              </lf-button>
            </template>
            <lf-dropdown-item v-if="!isSynced" @click="sync()">
              <div class="flex w-66 gap-2 items-start">
                <lf-icon name="arrows-rotate" type="regular" :size="16" />
                <div>
                  <p class="text-small mb-0.5">
                    Sync organization
                  </p>
                  <p class="text-tiny text-gray-500">
                    Includes all existing and future repositories within this
                    organization
                  </p>
                </div>
              </div>
            </lf-dropdown-item>
            <lf-dropdown-item v-else @click="unsync()">
              <div class="flex w-66 gap-2 items-start">
                <lf-icon name="ban" type="regular" :size="16" />
                <div>
                  <p class="text-small mb-0.5">
                    Stop syncing
                  </p>
                  <p class="text-tiny text-gray-500">
                    Synchronization will stop for future repositories from this
                    organization
                  </p>
                </div>
              </div>
            </lf-dropdown-item>
            <lf-dropdown-item type="danger" @click="remove()">
              <lf-icon name="circle-minus" type="regular" :size="16" />
              Remove organization (all repositories)
            </lf-dropdown-item>
          </lf-dropdown>
        </div>
      </div>
    </div>
    <slot />
  </div>
</template>

<script lang="ts" setup>
import {
  GitHubOrganization,
  GitHubRepository,
  GitHubSettingsRepository,
} from '@/config/integrations/github-nango/types/GithubSettings';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import { computed } from 'vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import { GithubApiService } from '@/config/integrations/github-nango/services/github.api.service';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  organizations: GitHubOrganization[];
  repositories: GitHubRepository[];
  organization: GitHubOrganization;
}>();

const emit = defineEmits<{(e: 'update:organizations', value: GitHubOrganization[]): void;
  (e: 'update:repositories', value: GitHubRepository[]): void;
  (e: 'remove-mapping', repoUrl: string[]): void;
}>();

const orgs = computed<GitHubOrganization[]>({
  get: () => props.organizations,
  set: (value) => emit('update:organizations', value),
});

const repos = computed<GitHubRepository[]>({
  get: () => props.repositories,
  set: (value) => emit('update:repositories', value),
});

const isSynced = computed(() => orgs.value.some((org) => org.url === props.organization.url));

const sync = () => {
  orgs.value.push({
    ...props.organization,
    updatedAt: dateHelper().toISOString(),
  });
  GithubApiService.getOrganizationRepositories(props.organization.name).then(
    (res) => {
      const newRepositories = (res as GitHubSettingsRepository[])
        .filter(
          (r: GitHubSettingsRepository) => !repos.value.some(
            (repo: GitHubSettingsRepository) => repo.url === r.url,
          ),
        )
        .map((r: GitHubSettingsRepository) => ({
          ...r,
          org: props.organization,
          updatedAt: dateHelper().toISOString(),
        }));
      repos.value = [...repos.value, ...newRepositories];
    },
  );
};

const unsync = () => {
  orgs.value = orgs.value.filter((org) => org.url !== props.organization.url);
};

const remove = () => {
  const reposToRemove = repos.value
    .filter((repo) => repo.org!.url === props.organization.url)
    .map((repo) => repo.url);

  unsync();
  repos.value = repos.value.filter(
    (repo) => repo.org!.url !== props.organization.url,
  );

  // Emit all repository URLs that need to be removed from mapping
  emit('remove-mapping', reposToRemove);
};
</script>

<script lang="ts">
export default {
  name: 'LfGithubSettingsOrgItem',
};
</script>
