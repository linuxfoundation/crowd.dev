<template>
  <div>
    <div class="flex items-center gap-1">
      <div
        class="text-gray-600 text-2xs flex items-center leading-5 font-medium"
      >
        <lf-icon name="house-building" :size="16" class="!text-gray-600 mr-1 flex items-center" />
        <span>Organization:&nbsp;</span>
        <span class="max-w-3xs truncate">{{ organization.name }}</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'LfLinkedinParams',
});

const props = defineProps({
  integration: {
    type: Object,
    default: () => {},
  },
});

const organization = computed<{ id: string; name: string; inUse: boolean }>(
  () => props.integration.settings.organizations.filter(
    (org: { id: string; name: string; inUse: boolean }) => org?.inUse,
  )?.[0],
);
</script>
