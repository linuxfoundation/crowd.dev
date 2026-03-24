<template>
  <div class="flex justify-center">
    <div class="pt-10 flex flex-col items-center w-full max-w-100 gap-4">
      <lf-icon name="book" type="regular" :size="50" class="text-gray-400" />
      <h6 class="text-center">
        Sync and map GitHub repositories
      </h6>
      <div class="flex flex-col gap-4">
        <div class="text-small text-gray-600 flex flex-row gap-4">
          <div class="text-xs font-medium bg-gray-100 rounded-full w-6 h-6 flex items-center justify-center shrink-0">
            1
          </div>
          Choose which repositories you want to track, or sync entire GitHub organizations;
        </div>
        <div class="text-small text-gray-600 flex flex-row gap-4">
          <div class="text-xs font-medium bg-gray-100 rounded-full w-6 h-6 flex items-center justify-center shrink-0">
            2
          </div>
          Map each repository with the corresponding project.
        </div>
      </div>
      <lf-button type="primary-ghost" @click="emit('add')">
        <lf-icon name="plus" type="regular" />
        Sync repositories/organizations
      </lf-button>
    </div>
  </div>
</template>

<script lang="ts" setup>
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const emit = defineEmits<{(e: 'add'): void }>();
</script>

<script lang="ts">
export default {
  name: 'LfGithubSettingsEmpty',
};
</script>
