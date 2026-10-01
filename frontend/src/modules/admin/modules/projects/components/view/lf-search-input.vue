<template>
  <el-input
    v-model="searchInput"
    clearable
    :placeholder="placeholder"
    :prefix-icon="SearchIcon"
    @input="debouncedChange"
  />
</template>

<script setup>
import { h, ref, watch } from 'vue';
import debounce from 'lodash/debounce';
import { useRoute } from 'vue-router';

defineProps({
  placeholder: {
    type: String,
    default: () => null,
  },
});
const emit = defineEmits(['onChange']);
const route = useRoute();
const searchInput = ref();

const SearchIcon = h(
  'i', // type
  { class: 'fa-light fa-magnifying-glass' }, // props
  [],
);

const debouncedChange = debounce((value) => {
  emit('onChange', value);
}, 300);

watch(() => route.query.activeTab, (newActiveTab, oldActiveTab) => {
  if (newActiveTab !== oldActiveTab) {
    searchInput.value = null;
  }
});
</script>

<script>
export default {
  name: 'AppLfSearchInput',
};
</script>
