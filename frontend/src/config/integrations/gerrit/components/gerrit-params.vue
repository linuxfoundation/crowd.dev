<template>
  <div>
    <div class="flex items-center gap-1">
      <el-popover trigger="hover" placement="top" popper-class="!w-72">
        <template #reference>
          <div
            class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
          >
            <lf-svg name="git-repository" class="w-4 h-4 !text-gray-600 mr-1 flex items-center" />
            {{ pluralize("repository", repositories.length, true) }}
          </div>
        </template>

        <p class="text-gray-400 text-sm font-semibold mb-4">
          Gerrit repositories
        </p>
        <div class="max-h-44 overflow-auto -my-1 px-1">
          <article
            v-for="repository of repositories"
            :key="repository"
            class="flex items-center flex-nowrap mb-4 last:mb-0"
          >
            <lf-svg name="git-repository" class="w-4 h-4 mr-1 flex items-center" />

            <a
              :href="repository"
              target="_blank"
              rel="noopener noreferrer"
              class="text-gray-900 text-sm max-w-3xs truncate hover:underline"
            >
              {{ repository }}
            </a>
          </article>
        </div>
      </el-popover>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import pluralize from 'pluralize';
import LfSvg from '@/shared/svg/svg.vue';

defineOptions({
  name: 'AppGithubSettings',
});

const props = defineProps({
  integration: {
    type: Object,
    default: () => {},
  },
});

const repositories = computed<string[]>(
  () => props.integration.settings?.remote?.repoNames,
);
</script>
