<template>
  <div>
    <lf-badge type="secondary" class="!flex items-center gap-1 w-full !rounded-full !px-2.5">
      <lf-icon :name="props.icon || 'layer-group'" type="regular" />
      {{ pluralize(props.title || 'project', props.count, true) }}
    </lf-badge>
  </div>
</template>

<script lang="ts" setup>
import pluralize from 'pluralize';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';

defineOptions({
  name: 'LfProjectCount',
});

const props = defineProps<{
  count: number;
  icon?: string;
  title?: string;
}>();
</script>
