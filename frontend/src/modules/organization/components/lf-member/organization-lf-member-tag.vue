<template>
  <el-tooltip v-if="!!organization.lfxMembership" content="Active member organization of the LF or one of its projects" placement="top">
    <div class="text-primary-500 flex items-center gap-1 flex-nowrap">
      <lf-icon name="circle-bookmark" :class="iconFontSize" />
      <span v-if="!onlyShowIcon" class="font-semibold text-xs whitespace-nowrap">Member</span>
    </div>
  </el-tooltip>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Organization } from '../../types/Organization';

withDefaults(
  defineProps<{
    organization: Organization;
    onlyShowIcon?: boolean;
    iconFontSize?: 'text-sm' | 'text-base';
  }>(),
  {
    onlyShowIcon: false,
    iconFontSize: 'text-base',
  },
);
</script>
