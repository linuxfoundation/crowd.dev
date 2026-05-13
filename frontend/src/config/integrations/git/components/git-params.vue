<template>
  <div class="flex items-center gap-1">
    <lf-icon name="book" :size="16" class="text-gray-600 mr-1" />

    <el-popover v-if="nativeRepos.length > 0" trigger="hover" placement="top" popper-class="!w-72">
      <template #reference>
        <div
          class="text-gray-600 text-2xs flex items-center leading-5 font-semibold underline decoration-dotted cursor-default"
        >
          {{ nativeRepos.length }}
          {{ nativeRepos.length !== 1 ? "remote URLs" : "remote URL" }}
        </div>
      </template>

      <p class="text-gray-400 text-[13px] font-semibold mb-4">
        Git Remote URLs
      </p>
      <div class="max-h-44 overflow-auto -my-1 px-1">
        <article
          v-for="repository of nativeRepos"
          :key="repository"
          class="flex items-center flex-nowrap mb-4 last:mb-0"
        >
          <lf-icon name="book" :size="16" class="text-gray-600 mr-1" />

          <a
            :href="repository"
            target="_blank"
            rel="noopener noreferrer"
            class="text-gray-900 text-[13px] max-w-3xs truncate hover:underline"
          >
            {{ removeProtocolAndDomain(repository) }}
          </a>
        </article>
      </div>
    </el-popover>

    <span v-if="mirroredRepoUrls.length > 0 && nativeRepos.length > 0">/</span>

    <template
      v-for="(gmKey, idx) of Object.keys(groupedMirroredRepos)"
      :key="gmKey"
    >
      <el-popover
        trigger="hover"
        placement="top"
        popper-class="!w-72"
      >
        <template #reference>
          <div
            class="text-gray-600 text-2xs flex items-center leading-5 font-semibold underline decoration-dotted cursor-default"
          >
            {{ pluralize('repository', groupedMirroredRepos[gmKey].length, true) }} (via {{ getIntegrationName(gmKey) }})
          </div>
        </template>

        <p class="text-gray-400 text-[13px] font-semibold mb-4">
          Repositories
        </p>
        <div class="max-h-44 overflow-auto -my-1 px-1">
          <article
            v-for="repository of groupedMirroredRepos[gmKey]"
            :key="repository"
            class="flex items-center flex-nowrap mb-4 last:mb-0"
          >
            <lf-icon name="book" :size="16" class="text-gray-600 mr-1" />

            <a
              :href="repository"
              target="_blank"
              rel="noopener noreferrer"
              class="text-gray-900 text-[13px] max-w-3xs truncate hover:underline"
            >
              {{ removeProtocolAndDomain(repository) }}
            </a>
          </article>
        </div>
      </el-popover>

      <span v-if="idx < Object.keys(groupedMirroredRepos).length - 1">/</span>
    </template>
  </div>
</template>

<script setup lang="ts">
import {
  ref, onMounted, watch, computed,
} from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import pluralize from 'pluralize';
import { lfIntegrations } from '@/config/integrations';
import { Platform } from '@/shared/modules/platform/types/Platform';

const props = defineProps({
  integration: {
    type: Object,
    default: () => {},
  },
});

const allIntegrations = computed(() => lfIntegrations());

const repositories = ref<string[]>([]);

// Track mirrored repos (sourceIntegrationId != gitIntegrationId)
const mirroredRepoUrls = ref<string[]>([]);
const groupedMirroredRepos = ref<{ [key: string]: string[] }>({});
const nativeRepos = computed(() => repositories.value.filter((r) => !mirroredRepoUrls.value.includes(r)));

const fetchRepositories = () => {
  if (!props.integration?.id) return;

  IntegrationService.fetchGitMappings(props.integration)
    .then((res: any[]) => {
      const reposFromMappings = res.map((r) => r.url);
      repositories.value = reposFromMappings.length > 0 ? reposFromMappings : [...(props.integration.settings?.remotes || [])];
      groupedMirroredRepos.value = res.filter((r) => r.sourcePlatform !== Platform.GIT).reduce((acc, r) => {
        acc[r.sourcePlatform] = [...(acc[r.sourcePlatform] || []), r.url];
        return acc;
      }, {} as { [key: string]: string[] });

      mirroredRepoUrls.value = res
        .filter((r) => r.sourceIntegrationId !== r.gitIntegrationId)
        .map((r) => r.url);
    })
    .catch(() => {
      // Fallback to settings.remotes if API fails
      repositories.value = props.integration.settings?.remotes || [];
      mirroredRepoUrls.value = [];
      groupedMirroredRepos.value = {};
    });
};

onMounted(() => {
  fetchRepositories();
});

watch(() => props.integration, () => {
  fetchRepositories();
}, { deep: true });

const removeProtocolAndDomain = (url: string) => {
  try {
    const parsedUrl = new URL(url);
    let path = parsedUrl.pathname;

    // Remove leading slash and trailing .git if present
    path = path.replace(/^\//, '').replace(/\.git$/, '');

    // For SSH URLs (git@github.com:user/repo.git)
    if (parsedUrl.protocol === 'ssh:') {
      const sshParts = url.split(':');
      path = sshParts[1].replace(/\.git$/, '');
    }

    return path;
  } catch (error) {
    // If URL parsing fails, return the original string
    return url.replace(/^(https?:\/\/|git@)/, '').replace(/\.git$/, '');
  }
};

const getIntegrationName = (key: string) => allIntegrations.value[key.replace('-nango', '')]?.name || key;
</script>

<script lang="ts">
export default {
  name: 'AppGithubSettings',
};
</script>
