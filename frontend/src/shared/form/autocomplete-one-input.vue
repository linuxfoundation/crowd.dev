<template>
  <el-select
    :disabled="disabled"
    :loading="loading"
    :remote-method="handleSearch"
    :model-value="modelValue"
    :clearable="clearable"
    :default-first-option="true"
    :filterable="true"
    :placeholder="placeholder || ''"
    :remote="true"
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
      :created="true"
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

<script>
import isString from 'lodash/isString';
import { onSelectMouseLeave } from '@/utils/select';

const AUTOCOMPLETE_SERVER_FETCH_SIZE = 20;

export default {
  name: 'AppAutocompleteOneInput',

  props: {
    modelValue: {
      type: Object,
      default: () => null,
    },
    placeholder: {
      type: String,
      default: null,
    },
    fetchFn: {
      type: Function,
      default: () => {},
    },
    createFn: {
      type: Function,
      default: () => {},
    },
    disabled: {
      type: Boolean,
      default: false,
    },
    createIfNotFound: {
      type: Boolean,
      default: false,
    },
    createPrefix: {
      type: String,
      default: 'Create',
    },
    inputClass: {
      type: String,
      default: '',
    },
    allowCreate: {
      type: Boolean,
      default: false,
    },
    options: {
      type: Array,
      default: () => [],
    },
    clearable: {
      type: Boolean,
      default: true,
    },
    teleported: {
      type: Boolean,
      default: true,
    },
    storeKey: {
      type: String,
      default: null,
    },
    disableOption: {
      type: Function,
      default: () => {},
    },
  },
  emits: ['update:modelValue'],
  data() {
    return {
      loading: false,
      localOptions: this.options ? this.options : [],
      currentQuery: '',
      limit: AUTOCOMPLETE_SERVER_FETCH_SIZE,
    };
  },

  computed: {
    showCreateSuggestion() {
      return (
        this.createIfNotFound
        && this.currentQuery !== ''
        && !this.localOptions.some(
          (o) => o.label === this.currentQuery
            || o === this.currentQuery,
        )
      );
    },
    showEmptyMessage() {
      // Show empty message if request is not loading,
      // there are options or the only option is empty
      return (
        !this.loading
        && (!this.localOptions.length
          || (this.localOptions.length === 1
            && !this.localOptions[0].id))
      );
    },
  },

  async created() {
    await this.fetchAllResults();
  },

  methods: {
    async onChange(value) {
      if (
        this.currentQuery !== ''
        && this.createIfNotFound
        && !value
      ) {
        const newItem = await this.createFn(this.currentQuery);
        this.localOptions.push(newItem);
        this.$emit('update:modelValue', newItem);
      } else {
        this.$emit('update:modelValue', {
          ...value,
          ...this.storeKey && {
            [this.storeKey]: this.modelValue[this.storeKey],
          },
        } || null);
      }
    },

    async handleSearch(value) {
      if (!isString(value) && value === '') {
        return;
      }

      await this.handleServerSearch(value);
      this.localOptions.filter((item) => String(item.label || '')
        .toLowerCase()
        .includes(String(value || '').toLowerCase()));
    },

    async fetchAllResults() {
      this.loading = true;
      try {
        this.localOptions.length = 0;
        this.localOptions = await this.fetchFn({
          query: this.currentQuery,
          limit: AUTOCOMPLETE_SERVER_FETCH_SIZE,
        });
        this.loading = false;
      } catch (error) {
        console.error(error);
        this.loading = false;
      }
    },

    async handleServerSearch(value) {
      if (value === this.currentQuery) {
        return;
      }

      this.currentQuery = value;
      this.loading = true;

      try {
        this.localOptions.length = 0;
        this.localOptions = await this.fetchFn({
          query: value,
          limit: AUTOCOMPLETE_SERVER_FETCH_SIZE,
        });

        this.loading = false;
      } catch (error) {
        console.error(error);
        this.localOptions.length = 0;
        this.loading = false;
      }
    },

    onSelectMouseLeave,
  },
};
</script>
