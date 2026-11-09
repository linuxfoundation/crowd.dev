<template>
  <div>
    <div class="flex items-center gap-1">
      <el-popover trigger="hover" placement="top" popper-class="!w-72">
        <template #reference>
          <div class="flex flex-row gap-1">
            <div
              class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
            >
              <lf-icon name="house-building" class="!text-gray-600 mr-1 h-4 flex items-center" />
              {{ pluralize("organization", organizations.length, true) }}
            </div>
            •
            <div
              class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
            >
              {{ pluralize("user", users.length, true) }}
            </div>
          </div>
        </template>

        <div class="max-h-44 overflow-auto -my-1 px-1">
          <template v-if="organizations.length > 0">
            <p class="text-gray-400 text-sm font-semibold mb-4">
              DEV organizations
            </p>
            <article
              v-for="organization of organizations"
              :key="organization"
              class="flex items-center flex-nowrap mb-4 last:mb-0"
            >
              <lf-icon name="house-building" class="mr-1 h-4 flex items-center" />
              <span class="text-gray-900 text-sm max-w-3xs truncate">{{
                organization
              }}</span>
            </article>
          </template>

          <template v-if="users.length > 0">
            <p
              class="text-gray-400 text-sm font-semibold mb-4"
              :class="{ 'mt-4': organizations.length > 0 }"
            >
              DEV users
            </p>

            <article
              v-for="user of users"
              :key="user"
              class="flex items-center flex-nowrap mb-4 last:mb-0"
            >
              <lf-icon name="user" class="mr-1 h-4 flex items-center" />

              <span class="text-gray-900 text-sm max-w-3xs truncate">{{
                user
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

const organizations = computed<string[]>(
  () => props.integration.settings.organizations,
);
const users = computed<string[]>(() => props.integration.settings.users);
</script>
