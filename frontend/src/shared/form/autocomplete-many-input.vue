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
    @remove-tag="(tag) => $emit('remove-tag', tag)"
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

<script>
import { onSelectMouseLeave } from '@/utils/select';

const AUTOCOMPLETE_SERVER_FETCH_SIZE = 20;

export default {
  name: 'AppAutocompleteManyInput',
  props: {
    modelValue: {
      type: Array,
      default: () => [],
    },
    placeholder: {
      type: String,
      default: null,
    },
    fetchFn: {
      type: Function,
      default: null,
    },
    mapperFn: {
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
    collapseTags: {
      type: Boolean,
      default: false,
    },
    areOptionsInMemory: {
      type: Boolean,
      default: false,
    },
    parseModel: {
      type: Boolean,
      default: false,
    },
    optionsLimit: {
      type: Number,
      default: null,
    },
  },
  emits: ['remove-tag', 'update:modelValue'],
  data() {
    return {
      initialLoading: false,
      loading: false,
      localOptions: this.options ? this.options : [],
      filteredOptions: this.options ? this.options : [],
      currentQuery: '',
      isSelectFocused: false,
      limit: AUTOCOMPLETE_SERVER_FETCH_SIZE,
    };
  },

  computed: {
    showCreateSuggestion() {
      return (
        this.createIfNotFound
        && !!this.currentQuery
        && !this.localOptions.some(
          (o) => o.label === this.currentQuery
            || o === this.currentQuery,
        )
      );
    },
    showMoreResultsMessage() {
      return (
        !this.loading
        && this.availableOptions.length === this.limit
        && !this.areOptionsInMemory
      );
    },
    showEmptyMessage() {
      return (
        !this.loading
        && !this.availableOptions.length
        && !this.showCreateSuggestion
      );
    },
    showNoDataMessage() {
      return (
        !this.loading
        && !this.availableOptions.length
        && !this.currentQuery
      );
    },
    // Collapse tags if prop is set and if select component is closed/not focused
    shouldCollapseTags() {
      return this.collapseTags && !this.isSelectFocused;
    },
    // If parseModel is set to true, then the modelValue is initially an array and strings
    // and should be parsed into the component format
    model: {
      get() {
        return this.modelValue.map((v) => {
          if (this.parseModel) {
            return {
              id: v,
              label: v,
            };
          }

          return v;
        });
      },
      set(v) {
        const updatedValue = this.parseModel
          ? v.map((value) => value.label)
          : v;

        this.$emit('update:modelValue', updatedValue);
      },
    },
    // Rendered available options should be dependent on the current query
    availableOptions() {
      if (this.currentQuery) {
        return this.optionsLimit ? this.filteredOptions.slice(0, this.optionsLimit) : this.filteredOptions;
      }

      return this.optionsLimit ? this.localOptions.slice(0, this.optionsLimit) : this.localOptions;
    },
  },

  mounted() {
    if (this.fetchFn) {
      this.fetchAllResults();
    }
  },

  methods: {
    async onChange(value) {
      const { query } = this.$refs.input;

      if (!value.length) {
        this.model = [];
      }

      const promises = value.map(async (item) => {
        if (
          item === false
          && query !== ''
          && this.createIfNotFound
        ) {
          // item is created/typed from user
          const newItem = await this.createFn(query);

          this.localOptions.push(newItem);

          return newItem;
        }
        return item;
      });

      Promise.all(promises).then((values) => {
        this.model = values;
      });
    },

    async handleSearch(query) {
      if (query === this.currentQuery) {
        return;
      }

      this.currentQuery = String(query || '');

      if (!this.areOptionsInMemory) {
        await this.handleServerSearch(this.currentQuery);
      }

      this.filteredOptions = this.localOptions.filter(
        (item) => String(item.label || '')
          .toLowerCase()
          .includes(this.currentQuery.toLowerCase()),
      );
    },

    async fetchNotIncludedTags(response) {
      if (this.areOptionsInMemory) {
        return Promise.resolve();
      }

      const notIncluded = this.model.filter(
        (m) => response.findIndex((r) => r.id === m.id) === -1,
      );

      if (notIncluded.length) {
        return this.fetchFn({
          query: notIncluded,
          limit: this.limit,
        }).then((notIncludedResponse) => {
          this.localOptions.unshift(...notIncludedResponse);
        }).finally(() => Promise.resolve());
      }

      return Promise.resolve();
    },

    async fetchAllResults() {
      this.initialLoading = true;

      this.fetchFn({
        query: this.currentQuery,
        limit: this.limit,
      }).then((response) => {
        this.localOptions = response;

        this.fetchNotIncludedTags(response).finally(() => {
          this.initialLoading = false;
        });
      }).catch((error) => {
        console.error(error);
        this.initialLoading = false;
      });
    },

    async handleServerSearch(value) {
      this.loading = true;

      try {
        const response = await this.fetchFn({
          query: value,
          limit: this.limit,
        });

        this.localOptions = response;

        if (!value) {
          await this.fetchNotIncludedTags(response);
        }

        this.loading = false;
      } catch (error) {
        console.error(error);
        this.localOptions = [];
        this.loading = false;
      }
    },

    onVisibleChange(value) {
      if (!this.collapseTags) {
        return;
      }

      this.isSelectFocused = value;

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
    },

    onSelectMouseLeave,
  },
};
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
