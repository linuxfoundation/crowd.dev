<template>
  <div class="flex gap-3">
    <div
      class="w-1/3 flex flex-col gap-1 justify-start mt-0.5"
    >
      <div class="flex items-center leading-tight">
        <div
          class="text-gray-900 text-xs font-medium mr-2"
        >
          {{ label }}
        </div>
        <el-tooltip v-if="isEnrichmentField" content="Source: Enrichment" placement="top" trigger="hover">
          <lf-svg name="source" class="h-3 w-3" />
        </el-tooltip>
      </div>
      <span
        class="text-2xs text-gray-500 leading-none"
      >{{ type }}</span>
    </div>
    <el-form-item class="grow">
      <slot />
    </el-form-item>
  </div>
</template>

<script setup lang="ts">
import LfSvg from '@/shared/svg/svg.vue';

defineOptions({
  name: 'AppOrganizationFormItem',
});

withDefaults(defineProps<{
  label?: string | null;
  type?: string | null;
  isEnrichmentField?: boolean;
}>(), {
  label: null,
  type: null,
  isEnrichmentField: false,
});
</script>

<style lang="scss">
.form-enrichment-badge {
  @apply w-5 h-5 bg-gray-100 rounded-full flex items-center justify-center;
  svg {
    @apply h-4 w-4 overflow-visible flex items-center justify-center leading-none;
  }
}

.multi-select-field .el-select__tags {
  @apply h-7;
}
</style>
