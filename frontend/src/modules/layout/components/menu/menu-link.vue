<template>
  <el-tooltip
    :hide-after="50"
    effect="dark"
    placement="right"
    raw-content
    :content="props.title"
    :disabled="!props.isCollapsed"
  >
    <component
      :is="props.disabled ? 'span' : 'router-link'"
      :to="props.to"
      v-bind="$attrs"
      class="l-menu__link"
      active-class="is-active"
      :class="{ 'is-disabled': props.disabled }"
    >
      <lf-icon :name="props.icon" :size="16" />
      <span class="transition" :class="!props.isCollapsed ? 'opacity-100' : 'opacity-0'">
        {{ props.title }}
      </span>
    </component>
  </el-tooltip>
</template>

<script setup lang="ts">
import type { RouteLocationRaw } from 'vue-router';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  isCollapsed: boolean;
  to: RouteLocationRaw,
  icon: string,
  title: string,
  disabled?: boolean,
}>();
</script>

<script lang="ts">
export default {
  name: 'LfMenuLink',
};
</script>
