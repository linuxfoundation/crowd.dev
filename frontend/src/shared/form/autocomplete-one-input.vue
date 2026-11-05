<template>
  <el-select
    :disabled="disabled"
    :loading="loading"
    :remote-method="handleSearch"
    :model-value="modelValue"
    :clearable="clearable"
    default-first-option
    filterable
    :placeholder="placeholder || ''"
    remote
    :reserve-keyword="false"
    :allow-create="allowCreate"
    fit-input-width
    value-key="id"
    :class="inputClass"
    :teleported="teleported"
    @change="onChange"
  >
    <template
      v-for="(_, name) in $slots"
      #[name]="slotData"
    >
      <slot
        :name="name"
        v-bind="slotData"
      />
    </template>
    <el-option
      v-show="showCreateSuggestion"
      :label="currentQuery"
      created
      @mouseleave="onSelectMouseLeave"
    >
      <span class="prefix">{{ createPrefix }}</span>
      <span>{{ currentQuery }}</span>
    </el-option>
    <template
      v-for="record in localOptions"
      :key="record.id"
    >
      <el-tooltip
        v-if="record.id"
        :key="record.id"
        :disabled="!disableOption(record)"
        content="Active member organizations of the Linux Foundation can't be merged into other organizations."
        placement="top"
      >
        <span>
          <el-option
            :value="record"
            :label="record.label"
            :disabled="disableOption(record)"
            class="!px-5"
            :class="{
              'hover:!bg-transparent': disableOption(record),
            }"
            @mouseleave="onSelectMouseLeave"
          >
            <slot name="option" :item="record">
              <span class="text-ellipsis overflow-hidden">
                {{ record.label }}
              </span>
            </slot>
          </el-option>
        </span>
      </el-tooltip>
    </template>
    <div
      v-if="!loading && localOptions.length === limit"
      class="px-5 text-gray-400 text-2xs w-full h-8 flex items-center"
    >
      Type to search for more results
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
import { computed, ref } from 'vue';
import isString from 'lodash/isString';
import { onSelectMouseLeave } from '@/utils/select';

interface AutocompleteOption {
  id?: string | number;
  label?: string;
  [key: string]: unknown;
}

defineOptions({ name: 'AppAutocompleteOneInput' });

const props = withDefaults(defineProps<{
  modelValue?: object | null;
  placeholder?: string | null;
  fetchFn?:(params: { query: string; limit: number }) => unknown;
  createFn?:(query: string) => unknown;
  disabled?: boolean;
  createIfNotFound?: boolean;
  createPrefix?: string;
  inputClass?: string;
  allowCreate?: boolean;
  options?: AutocompleteOption[];
  clearable?: boolean;
  teleported?: boolean;
  storeKey?: string | null;
  disableOption?:(option: AutocompleteOption) => unknown;
}>(), {
  modelValue: () => null,
  placeholder: null,
  fetchFn: () => {},
  createFn: () => {},
  disabled: false,
  createIfNotFound: false,
  createPrefix: 'Create',
  inputClass: '',
  allowCreate: false,
  options: () => [],
  clearable: true,
  teleported: true,
  storeKey: null,
  disableOption: () => {},
});

const emit = defineEmits<{(e: 'update:modelValue', value: object | null): void}>();

const AUTOCOMPLETE_SERVER_FETCH_SIZE = 20;

const loading = ref(false);
const localOptions = ref<AutocompleteOption[]>(props.options ? props.options : []);
const currentQuery = ref('');
const limit = ref(AUTOCOMPLETE_SERVER_FETCH_SIZE);

const showCreateSuggestion = computed(() => (
  props.createIfNotFound
  && currentQuery.value !== ''
  && !localOptions.value.some(
    (o) => o.label === currentQuery.value
      || (o as unknown) === currentQuery.value,
  )
));

const showEmptyMessage = computed(() => (
  !loading.value
  && (!localOptions.value.length
    || (localOptions.value.length === 1
      && !localOptions.value[0].id))
));

const onChange = async (value: AutocompleteOption | string | undefined) => {
  if (
    currentQuery.value !== ''
    && props.createIfNotFound
    && !value
  ) {
    const newItem = await props.createFn(currentQuery.value) as AutocompleteOption;
    localOptions.value.push(newItem);
    emit('update:modelValue', newItem);
  } else {
    emit('update:modelValue', {
      ...(value as AutocompleteOption),
      ...props.storeKey && {
        [props.storeKey]: (props.modelValue as Record<string, unknown>)[props.storeKey],
      },
    });
  }
};

const handleServerSearch = async (value: string) => {
  if (value === currentQuery.value) {
    return;
  }

  currentQuery.value = value;
  loading.value = true;

  try {
    localOptions.value.length = 0;
    localOptions.value = await props.fetchFn({
      query: value,
      limit: AUTOCOMPLETE_SERVER_FETCH_SIZE,
    }) as AutocompleteOption[];

    loading.value = false;
  } catch (error) {
    console.error(error);
    localOptions.value.length = 0;
    loading.value = false;
  }
};

const handleSearch = async (value: string) => {
  if (!isString(value) && (value as unknown) === '') {
    return;
  }

  await handleServerSearch(value);
  localOptions.value.filter((item) => String(item.label || '')
    .toLowerCase()
    .includes(String(value || '').toLowerCase()));
};

const fetchAllResults = async () => {
  loading.value = true;
  try {
    localOptions.value.length = 0;
    localOptions.value = await props.fetchFn({
      query: currentQuery.value,
      limit: AUTOCOMPLETE_SERVER_FETCH_SIZE,
    }) as AutocompleteOption[];
    loading.value = false;
  } catch (error) {
    console.error(error);
    loading.value = false;
  }
};

fetchAllResults();
</script>
