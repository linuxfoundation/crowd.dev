<template>
  <div class="flex items-center gap-1">
    <el-popover v-if="mappings.length > 0" trigger="hover" placement="top" popper-class="!w-auto">
      <template #reference>
        <slot name="trigger">
          <div
            class="text-neutral-600 text-2xs flex items-center leading-5"
          >
            <lf-icon name="book" :size="16" class="text-neutral-600 mr-2" />
            <span class="font-semibold underline decoration-dotted cursor-default">
              {{ pluralize("repository", Object.keys(mappings).length, true) }}
            </span>
          </div>
        </slot>
      </template>

      <p class="text-neutral-400 text-sm font-semibold mb-4">
        <slot name="popupTitle" />
      </p>
      <div class="-my-1 px-1 max-h-44 overflow-auto">
        <div v-for="group of Object.keys(groupedMappings)" :key="group" class="mb-2.5">
          <div class="text-neutral-400 text-sm font-semibold mb-2">
            {{ group }}
          </div>
          <article
            v-for="mapping of groupedMappings[group]"
            :key="mapping.url"
            class="py-2 flex items-center flex-nowrap"
          >
            <img
              :src="gitRepositoryIcon"
              width="16"
              height="16"
              class="text-neutral-600 mr-2"
              alt="Github repository"
            />
            <a
              :href="mapping.url"
              target="_blank"
              rel="noopener noreferrer"
              class="text-xs leading-5 max-w-3xs truncate hover:underline !text-black"
            >
              /{{ repoNameFromUrl(mapping.url) }}
            </a>
          </article>
        </div>
      </div>
    </el-popover>
    <slot v-else name="trigger">
      <div
        class="text-neutral-600 text-2xs flex items-center leading-5"
      >
        <lf-icon name="book" :size="16" class="text-neutral-600 mr-1" />
        <span class="font-semibold underline decoration-dotted cursor-default">
          {{ pluralize("repository", Object.keys(mappings).length, true) }}
        </span>
      </div>
    </slot>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import pluralize from 'pluralize';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { IntegrationMapping } from '@/modules/admin/modules/integration/types/Integration';

const gitRepositoryIcon = new URL(
  '@/assets/images/integrations/git-repository-line.svg',
  import.meta.url,
).href;

const props = defineProps<{
  mappings: IntegrationMapping[];
}>();

const repoNameFromUrl = (url: string) => url.split('/').at(-1);

const groupedMappings = computed(() => props.mappings.reduce((acc, mapping) => {
  acc[mapping.segment.name] = [...(acc[mapping.segment.name] || []), mapping];
  return acc;
}, {} as { [key: string]: IntegrationMapping[] }));
</script>

<script lang="ts">
export default {
  name: 'LfGithubMappingsDisplay',
};
</script>
