<template>
  <el-popover v-model:visible="open" placement="bottom-start" size="large" width="39rem" popper-class="!p-0" trigger="click">
    <template #reference>
      <el-select
        v-model="currentSelection"
        class="w-full"
        popper-class="attribute-dropdown-popper"
        clearable
        placeholder="Select option"
      />
    </template>

    <div class="border-b border-gray-100 p-2">
      <el-input
        v-model="search"
        placeholder="Search..."
        class="filter-dropdown-search"
        data-qa="filter-list-search"
      >
        <template #prefix>
          <lf-icon name="magnifying-glass" :size="16" />
        </template>
      </el-input>
    </div>
    <div class="max-h-80 overflow-auto px-2 py-3">
      <!-- DEFAULT ATTRIBUTES -->
      <template v-if="filteredDefaultOptions.length > 0">
        <div class="el-dropdown-title !my-3 !-ml-1">
          Default Attributes
        </div>
        <article
          v-for="attribute in filteredDefaultOptions"
          :key="attribute.name"
          class="mb-1 p-3 rounded flex justify-between items-center transition whitespace-nowrap h-10 hover:bg-gray-50 text-xs !cursor-pointer"
          data-qa="filter-list-item-custom"
          @click="selectItem(attribute, 'default')"
        >
          <span class="!text-gray-900">{{ attribute.label }}</span>
        </article>
      </template>

      <div
        v-if="filteredDefaultOptions.length === 0 && filteredCustomOptions.length === 0"
        class="el-dropdown-title !mt-2"
      >
        No results
      </div>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import {
  computed,
  defineProps, ref,
  watch,
} from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

type Attribute = {
  label: string,
  name: string,
};

const props = defineProps<{
  modelValue: {
    default: Attribute[],
    custom: Attribute[],
}}>();

const emit = defineEmits(['update:modelValue', 'change', 'clear']);

const currentSelection = ref<string>('');
const open = ref<boolean>(false);
const search = ref<string>('');

const matchesSearch = (label: string, query: string): boolean => label.toLowerCase().includes(query.toLowerCase());

const filteredDefaultOptions = computed(() => props.modelValue.default.filter((filter) => matchesSearch(filter.label, search.value)));

const filteredCustomOptions = computed(() => props.modelValue.custom.filter((filter) => matchesSearch(filter.label, search.value)));

watch(currentSelection, (value) => {
  if (value === '') {
    emit('clear');
  }
});

const selectItem = (item: Attribute, source: string) => {
  currentSelection.value = item.label;
  search.value = '';
  open.value = false;
  emit('change', { [source]: item });
};

</script>

<script lang="ts">
export default {
  name: 'AppBulkEditAttributeDropdown',
};
</script>

<style>
.attribute-dropdown-popper {
  display: none !important;
}
</style>
