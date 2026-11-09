<template>
  <lf-icon
    :name="icons[platform] ? icons[platform].name : 'fingerprint'"
    :type="icons[platform] ? icons[platform].type : 'light'"
    class="text-gray-900"
    :class="iconClass"
  />
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineOptions({
  name: 'AppPlatformIcon',
});

const props = defineProps<{
  platform: string;
  size: string;
}>();

const icons: { [key: string]: { name: string, type: string } } = {
  domains: { name: 'window', type: 'regular' },
  emails: { name: 'envelope', type: 'light' },
  phoneNumbers: { name: 'phone', type: 'light' },
};

const iconClass = computed(() => {
  if (props.size === 'large') {
    return 'text-lg leading-5';
  }

  return 'text-base leading-4';
});
</script>
