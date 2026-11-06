<template>
  <el-select
    ref="input"
    :disabled="disabled || initialLoading"
    :loading="loading || initialLoading"
    :remote-method="handleSearch"
    :model-value="initialLoading ? null : model"
    clearable
    default-first-option
    filterable
    multiple
    :placeholder="placeholder || ''"
    remote
    :reserve-keyword="false"
    :allow-create="allowCreate"
    :suffix-icon="initialLoading ? 'app-loader' : null"
    :remote-show-suffix="initialLoading"
    :collapse-tags="shouldCollapseTags"
    :collapse-tags-tooltip="shouldCollapseTags"
    value-key="id"
    class="autocomplete-many-input"
    :class="{
      [inputClass]: true,
      collapsed: collapseTags && !isSelectFocused,
      expand: collapseTags && isSelectFocused,
      empty: collapseTags && !model.length,
    }"
    @change="onChange"
    @remove-tag="(tag: unknown) => emit('remove-tag', tag)"
    @visible-change="onVisibleChange"
  >
    <el-option
      v-show="showCreateSuggestion"
      :label="currentQuery"
      created
      @mouseleave="onSelectMouseLeave"
    >
      <span class="prefix">{{ createPrefix }}</span>
      <span>{{ currentQuery }}</span>
    </el-option>
    <el-option
      v-for="record in availableOptions"
      :key="record.id"
      :label="record.label"
      :value="record"
      @mouseleave="onSelectMouseLeave"
    >
      <slot name="option" :item="record" />
    </el-option>
    <div
      v-if="showMoreResultsMessage"
      class="px-5 text-gray-400 text-2xs w-full h-8 flex items-center"
    >
      Type to search for more results
    </div>
    <div
      v-else-if="showNoDataMessage"
      class="px-5 text-gray-400 text-xs w-full h-10 flex items-center justify-center"
    >
      No data
    </div>
    <div
      v-else-if="showEmptyMessage"
      class="px-5 text-gray-400 text-xs w-full h-10 flex items-center justify-center"
    >
      No matches found
    </div>
  </el-select>
</template>

<script setup lang="ts">
import {
  computed, onMounted, ref,
} from 'vue';
import { onSelectMouseLeave } from '@/utils/select';

interface AutocompleteOption {
  id?: unknown;
  label?: string;
  [key: string]: unknown;
}

defineOptions({ name: 'AppAutocompleteManyInput' });

const props = withDefaults(defineProps<{
  modelValue?: unknown[];
  placeholder?: string | null;
  fetchFn?:((params: { query: string | AutocompleteOption[]; limit: number }) => Promise<AutocompleteOption[]>) | null;
  mapperFn?:(...args: unknown[]) => unknown;
  createFn?:(query: string) => unknown;
  disabled?: boolean;
  createIfNotFound?: boolean;
  createPrefix?: string;
  inputClass?: string;
  allowCreate?: boolean;
  options?: AutocompleteOption[];
  collapseTags?: boolean;
  areOptionsInMemory?: boolean;
  parseModel?: boolean;
  optionsLimit?: number | null;
}>(), {
  modelValue: () => [],
  placeholder: null,
  fetchFn: null,
  mapperFn: () => {},
  createFn: () => {},
  disabled: false,
  createIfNotFound: false,
  createPrefix: 'Create',
  inputClass: '',
  allowCreate: false,
  options: () => [],
  collapseTags: false,
  areOptionsInMemory: false,
  parseModel: false,
  optionsLimit: null,
});

const emit = defineEmits<{(e: 'remove-tag', tag: unknown): void;
  (e: 'update:modelValue', value: unknown[]): void;
}>();

const AUTOCOMPLETE_SERVER_FETCH_SIZE = 20;

const input = ref<{ query: string } | null>(null);

const initialLoading = ref(false);
const loading = ref(false);
const localOptions = ref<AutocompleteOption[]>(props.options ? props.options : []);
const filteredOptions = ref<AutocompleteOption[]>(props.options ? props.options : []);
const currentQuery = ref('');
const isSelectFocused = ref(false);
const limit = ref(AUTOCOMPLETE_SERVER_FETCH_SIZE);

const showCreateSuggestion = computed(() => (
  props.createIfNotFound
  && !!currentQuery.value
  && !localOptions.value.some(
    (o) => o.label === currentQuery.value
      || (o as unknown) === currentQuery.value,
  )
));

// If parseModel is set to true, then the modelValue is initially an array and strings
// and should be parsed into the component format
const model = computed<AutocompleteOption[]>({
  get() {
    return props.modelValue.map((v) => {
      if (props.parseModel) {
        return {
          id: v,
          label: v as string,
        };
      }

      return v as AutocompleteOption;
    });
  },
  set(v) {
    const updatedValue = props.parseModel
      ? v.map((value) => value.label)
      : v;

    emit('update:modelValue', updatedValue);
  },
});

// Rendered available options should be dependent on the current query
const availableOptions = computed(() => {
  if (currentQuery.value) {
    return props.optionsLimit ? filteredOptions.value.slice(0, props.optionsLimit) : filteredOptions.value;
  }

  return props.optionsLimit ? localOptions.value.slice(0, props.optionsLimit) : localOptions.value;
});

const showMoreResultsMessage = computed(() => (
  !loading.value
  && availableOptions.value.length === limit.value
  && !props.areOptionsInMemory
));

const showEmptyMessage = computed(() => (
  !loading.value
  && !availableOptions.value.length
  && !showCreateSuggestion.value
));

const showNoDataMessage = computed(() => (
  !loading.value
  && !availableOptions.value.length
  && !currentQuery.value
));

// Collapse tags if prop is set and if select component is closed/not focused
const shouldCollapseTags = computed(() => props.collapseTags && !isSelectFocused.value);

const onChange = async (value: unknown[]) => {
  const { query } = input.value as { query: string };

  if (!value.length) {
    model.value = [];
  }

  const promises = value.map(async (item) => {
    if (
      item === false
      && query !== ''
      && props.createIfNotFound
    ) {
      // item is created/typed from user
      const newItem = await props.createFn(query) as AutocompleteOption;

      localOptions.value.push(newItem);

      return newItem;
    }
    return item as AutocompleteOption;
  });

  Promise.all(promises).then((values) => {
    model.value = values;
  });
};

const fetchNotIncludedTags = async (response: AutocompleteOption[]) => {
  if (props.areOptionsInMemory) {
    return Promise.resolve();
  }

  const notIncluded = model.value.filter(
    (m) => response.findIndex((r) => r.id === m.id) === -1,
  );

  if (notIncluded.length) {
    return props.fetchFn!({
      query: notIncluded,
      limit: limit.value,
    }).then((notIncludedResponse) => {
      localOptions.value.unshift(...notIncludedResponse);
    }).finally(() => Promise.resolve());
  }

  return Promise.resolve();
};

const handleServerSearch = async (value: string) => {
  loading.value = true;

  try {
    const response = await props.fetchFn!({
      query: value,
      limit: limit.value,
    });

    localOptions.value = response;

    if (!value) {
      await fetchNotIncludedTags(response);
    }

    loading.value = false;
  } catch (error) {
    console.error(error);
    localOptions.value = [];
    loading.value = false;
  }
};

const handleSearch = async (query: string) => {
  if (query === currentQuery.value) {
    return;
  }

  currentQuery.value = String(query || '');

  if (!props.areOptionsInMemory) {
    await handleServerSearch(currentQuery.value);
  }

  filteredOptions.value = localOptions.value.filter(
    (item) => String(item.label || '')
      .toLowerCase()
      .includes(currentQuery.value.toLowerCase()),
  );
};

const fetchAllResults = async () => {
  initialLoading.value = true;

  props.fetchFn!({
    query: currentQuery.value,
    limit: limit.value,
  }).then((response) => {
    localOptions.value = response;

    fetchNotIncludedTags(response).finally(() => {
      initialLoading.value = false;
    });
  }).catch((error) => {
    console.error(error);
    initialLoading.value = false;
  });
};

const onVisibleChange = (value: boolean) => {
  if (!props.collapseTags) {
    return;
  }

  isSelectFocused.value = value;

  // Scroll to the bottom of the list where cursor is focused
  if (value) {
    setTimeout(() => {
      const element = document.querySelector(
        '.autocomplete-many-input.expand .el-select__tags',
      );

      if (element) {
        element.scrollTop = element.scrollHeight;
      }
    }, 150);
  }
};

onMounted(() => {
  if (props.fetchFn) {
    fetchAllResults();
  }
});
</script>

<style lang="scss">
.autocomplete-many-input {
  &.collapsed {
    .el-input__wrapper {
      height: 40px;
    }

    .el-select__tags {
      top: 6px;
      transform: none;
    }
  }

  &.expand {
    .el-input__wrapper {
      height: 87px;
    }

    .el-select__tags {
      @apply overflow-auto;
      top: 6px;
      height: 80px;
      transform: none;
      align-items: start;
    }
  }

  &.empty {
    .el-input__wrapper {
      height: 40px;
    }

    .el-select__tags {
      height: 40px;
    }
  }
}
</style>
