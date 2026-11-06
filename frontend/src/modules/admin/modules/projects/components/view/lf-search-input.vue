<template>
  <el-input
    v-model="searchInput"
    clearable
    :placeholder="placeholder"
    :prefix-icon="SearchIcon"
    @input="debouncedChange"
  />
</template>

<script setup lang="ts">
import { h, ref, watch } from 'vue';
import debounce from 'lodash/debounce';
import { useRoute } from 'vue-router';

defineOptions({ name: 'AppLfSearchInput' });

withDefaults(defineProps<{
  placeholder?: string | null;
}>(), {
  placeholder: null,
});

const emit = defineEmits<{(e: 'onChange', value: string): void;
}>();

const route = useRoute();
const searchInput = ref<string | null>();

const SearchIcon = h(
  'i', // type
  { class: 'fa-light fa-magnifying-glass' }, // props
  [],
);

const debouncedChange = debounce((value: string) => {
  emit('onChange', value);
}, 300);

watch(() => route.query.activeTab, (newActiveTab, oldActiveTab) => {
  if (newActiveTab !== oldActiveTab) {
    searchInput.value = null;
  }
});
</script>
