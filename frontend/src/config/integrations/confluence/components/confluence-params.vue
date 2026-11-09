<template>
  <div>
    <div class="flex items-center gap-1">
      <el-popover trigger="hover" placement="top" popper-class="!w-72">
        <template #reference>
          <div class="flex items-center gap-1">
            <div
              class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
            >
              <lf-icon name="folders" class="!text-gray-600 mr-1 h-4 flex items-center" />
              1 organization
            </div>
            •
            <div
              class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
            >
              1 space
            </div>
          </div>
        </template>

        <div class="max-h-44 overflow-auto -my-1 px-1">
          <p class="text-gray-400 text-sm font-semibold mb-4">
            Confluence organizations
          </p>
          <article class="flex items-center flex-nowrap mb-4 last:mb-0">
            <lf-icon
              name="house-building"
              class="mr-1 h-4 flex items-center"
            />

            <span class="text-gray-900 text-sm max-w-3xs truncate">{{
              url
            }}</span>
          </article>

          <p class="text-gray-400 text-sm font-semibold mb-4 mt-4">
            Confluence spaces
          </p>

          <article class="flex items-center flex-nowrap mb-4 last:mb-0">
            <lf-icon name="folder" class="mr-1 h-4 flex items-center" />

            <span class="text-gray-900 text-sm max-w-3xs truncate">{{
              space?.name || space?.key || "Unnamed space"
            }}</span>
          </article>
        </div>
      </el-popover>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
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

const url = computed<string>(
  () => props.integration.settings.url
    ?? props.integration.settings?.remotes?.[0]
    ?? '',
);
const space = computed<{ id: string; name: string; key: string } | null>(
  () => props.integration.settings?.space,
);
</script>
