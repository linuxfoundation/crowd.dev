<template>
  <div class="flex justify-center">
    <div class="pt-10 flex flex-col items-center w-full max-w-100 gap-4">
      <lf-icon name="book" type="regular" :size="50" class="text-gray-400" />
      <h6 class="text-center">
        Sync Git repositories
      </h6>
      <div class="flex flex-col gap-4">
        <div class="text-small text-gray-600 text-center">
          Add a remote URL for each Git repository you want to connect
        </div>
      </div>
      <lf-button type="primary-ghost" @click="emit('add')">
        <lf-icon name="plus" type="regular" />
        Add remote URL
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
  name: 'LfGitSettingsEmpty',
};
</script>
