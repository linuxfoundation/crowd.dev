<template>
  <lf-dropdown width="10rem" placement="bottom-end">
    <template #trigger>
      <div class="flex py-1">
        <p class="text-small pr-1">
          <span class="font-semibold">Sort:</span> {{ label }}
        </p>
        <lf-icon name="chevron-down" :size="16" />
      </div>
    </template>
    <lf-dropdown-item
      v-for="option of options"
      :key="option.value"
      @click="sortBy = option.value"
    >
      {{ option.label }}
    </lf-dropdown-item>
  </lf-dropdown>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  sorting: string,
}>();

const emit = defineEmits<{(e: 'update:sorting', value: string): void}>();

const sortBy = computed({
  get: () => props.sorting,
  set: (value: string) => emit('update:sorting', value),
});

const label = computed(() => {
  const option = options.find((o) => o.value === sortBy.value);
  return option ? option.label : 'All';
});

interface SortOption {
  value: string;
  label: string;
}

const options: SortOption[] = [
  { value: 'name_ASC', label: 'Alphabetically' },
  { value: 'activityCount_DESC', label: 'Most activities' },
  { value: 'createdAt_DESC', label: 'Most recent' },
];
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsProjectsSorting',
};
</script>
