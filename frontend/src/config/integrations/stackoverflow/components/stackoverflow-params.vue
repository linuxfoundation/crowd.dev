<template>
  <div>
    <div class="flex items-center gap-1">
      <el-popover trigger="hover" placement="top" popper-class="!w-72">
        <template #reference>
          <div class="flex flex-row gap-1">
            <div
              class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
            >
              <lf-icon name="tag fa-rotate-90" :size="16" class="!text-gray-600 mr-1 flex items-center" />
              {{ pluralize('tag', tags.length, true) }}
            </div>
            <template v-if="tags.length > 0 && keywords.length > 0">
              •
            </template>
            <div
              class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
            >
              {{ pluralize('keyword', keywords.length, true) }}
            </div>
          </div>
        </template>

        <div class="max-h-44 overflow-auto -my-1 px-1">
          <template v-if="tags.length > 0">
            <p class="text-gray-400 text-sm font-semibold mb-4">
              Stack Overflow tags
            </p>
            <article
              v-for="tag of tags"
              :key="tag"
              class="flex items-center flex-nowrap mb-4 last:mb-0"
            >
              <lf-icon name="tag fa-rotate-90" :size="16" class="mr-1 flex items-center" />
              <span class="text-gray-900 text-sm max-w-3xs truncate">{{
                tag
              }}</span>
            </article>
          </template>

          <template v-if="keywords.length > 0">
            <p
              class="text-gray-400 text-sm font-semibold mb-4"
              :class="{ 'mt-4': tags.length > 0 }"
            >
              Stack Overflow keywords
            </p>

            <article
              v-for="keyword of keywords"
              :key="keyword"
              class="flex items-center flex-nowrap mb-4 last:mb-0"
            >
              <lf-icon name="hashtag" :size="16" class="mr-1 flex items-center" />

              <span class="text-gray-900 text-sm max-w-3xs truncate">{{
                keyword
              }}</span>
            </article>
          </template>
        </div>
      </el-popover>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import pluralize from 'pluralize';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'AppGithubSettings',
});

const props = defineProps({
  integration: {
    type: Object,
    default: () => {},
  },
});

const tags = computed<string[]>(() => props.integration.settings.tags);
const keywords = computed<string[]>(() => props.integration.settings.keywords);
</script>
