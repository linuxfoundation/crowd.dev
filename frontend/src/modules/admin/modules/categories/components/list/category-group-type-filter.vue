<template>
  <lf-dropdown placement="bottom-start" width="9rem">
    <template #trigger>
      <lf-button type="secondary" class="!shadow-sm">
        <div class="flex items-center">
          <p>
            <span class="text-medium text-gray-400">Type: </span>
            <span v-if="model === ''" class="font-normal">All</span>
            <span v-else-if="model === CategoryGroupType.VERTICAL" class="font-normal">Industry</span>
            <span v-else-if="model === CategoryGroupType.HORIZONTAL" class="font-normal">Stack</span>
          </p>
        </div>
      </lf-button>
    </template>

    <lf-dropdown-item
      :selected="model === ''"
      @click="model = ''"
    >
      All
    </lf-dropdown-item>
    <lf-dropdown-separator />
    <lf-dropdown-item
      :selected="model === CategoryGroupType.VERTICAL"
      @click="model = CategoryGroupType.VERTICAL"
    >
      Industry
    </lf-dropdown-item>
    <lf-dropdown-item
      :selected="model === CategoryGroupType.HORIZONTAL"
      @click="model = CategoryGroupType.HORIZONTAL"
    >
      Stack
    </lf-dropdown-item>
  </lf-dropdown>
</template>

<script lang="ts" setup>
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { computed } from 'vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import { CategoryGroupType } from '@/modules/admin/modules/categories/types/CategoryGroup';

defineOptions({
  name: 'LfCategoryGroupTypeFilter',
});

const props = defineProps<{
  modelValue: string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void;
}>();

const model = computed<string>({
  get: () => props.modelValue,
  set: (value: string) => emit('update:modelValue', value),
});
</script>
