<template>
  <div :class="Object.keys(props.config).length > 0 ? 'mb-4' : ''">
    <div class="flex justify-end pb-4 gap-4">
      <lf-filter-search v-if="props.searchConfig" v-model="filters.search" :placeholder="props.searchConfig.placeholder" class="!h-9" />
      <lf-filter-dropdown
        v-if="Object.keys(props.config).length > 0"
        v-model="filterList"
        :config="props.config"
        :custom-config="props.customConfig || {}"
        @open="open = $event"
      />
      <lf-button
        v-if="isDeveloperModeActive"
        type="secondary"
        class="!bg-purple-100 !text-purple-600"
        @click="copyToClipboard"
      >
        <lf-icon name="clipboard" />
        <span>Copy JSON query</span>
      </lf-button>
      <slot name="actions" />
    </div>
    <div class="flex items-center flex-wrap">
      <template v-for="(filter, fi) of filterList" :key="filter">
        <!-- Operator -->
        <el-tooltip
          effect="dark"
          :content="`${filters.relation} → ${filters.relation === 'and' ? 'or' : 'and'}`"
          placement="top"
          :disabled="props.lockRelation"
        >
          <div
            v-if="fi > 0"
            :click="!props.lockRelation ? 'cursor-pointer hover:bg-gray-100' : ''"
            class="border text-xs border-gray-100 rounded-md shadow justify-center
            h-8 flex font-medium items-center py-1 px-2 bg-white transition mr-3 mb-4"
            @click="switchOperator"
          >
            {{ filters.relation }}
          </div>
        </el-tooltip>

        <!-- Filter -->
        <lf-filter-item
          v-model="filters[filter]"
          v-model:open="open"
          :config="configuration[filter]"
          class="mr-3 mb-4"
          @remove="removeFilter(filter)"
        />
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  computed, defineProps, onMounted, ref, watch,
} from 'vue';
import type { Filter, FilterConfig } from '@/shared/modules/filters/types/FilterConfig';
import LfFilterDropdown from '@/shared/modules/filters/components/FilterDropdown.vue';
import LfFilterItem from '@/shared/modules/filters/components/FilterItem.vue';
import LfFilterSearch from '@/shared/modules/filters/components/FilterSearch.vue';
import { filterQueryService } from '@/shared/modules/filters/services/filter-query.service';
import type { SearchFilterConfig } from '@/shared/modules/filters/types/filterTypes/SearchFilterConfig';
import { useRoute, useRouter } from 'vue-router';
import { filterApiService } from '@/shared/modules/filters/services/filter-api.service';
import type { FilterQuery } from '@/shared/modules/filters/types/FilterQuery';
import type { SavedViewsConfig } from '@/shared/modules/saved-views/types/SavedViewsConfig';
import { useUserStore } from '@/modules/user/store/pinia';

import { ToastStore } from '@/shared/message/notification';
import { storeToRefs } from 'pinia';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps<{
  modelValue: Filter,
  config: Record<string, FilterConfig>,
  customConfig?: Record<string, FilterConfig>,
  searchConfig?: SearchFilterConfig,
  savedViewsConfig?: SavedViewsConfig,
  hash?: string,
  lockRelation?: boolean,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: Filter), (e: 'fetch', value: FilterQuery), }>();

const { trackEvent } = useProductTracking();
const router = useRouter();
const route = useRoute();

const userStore = useUserStore();
const { isDeveloperModeActive } = storeToRefs(userStore);

const open = ref('');

const filters = computed<Filter>({
  get() {
    return props.modelValue;
  },
  set(value: Filter) {
    alignFilterList(value);
    emit('update:modelValue', value);
  },
});

const configuration = computed(() => ({
  ...props.config,
  ...props.customConfig,
}));

const filterList = ref<string[]>([]);

const switchOperator = () => {
  if (props.lockRelation) {
    return;
  }
  filters.value.relation = filters.value.relation === 'and' ? 'or' : 'and';
};

const alignFilterList = (value: Filter) => {
  const {
    settings, search, relation, order, ...filterValues
  } = value;
  filterList.value = Object.keys(filterValues);
};

const removeFilter = (key: string) => {
  open.value = '';
  filterList.value = filterList.value.filter((el) => el !== key);
  delete filters.value[key];
};

const { setQuery, parseQuery } = filterQueryService();
const { buildApiFilter } = filterApiService();

const savedFilter = ref({});
const fetch = (value: Filter) => {
  if (JSON.stringify(value) === JSON.stringify(savedFilter.value)) {
    return;
  }

  savedFilter.value = { ...value };

  const data = buildApiFilter(value, { ...props.config, ...props.customConfig }, props.searchConfig, props.savedViewsConfig);
  emit('fetch', data);
};

watch(
  () => filters.value,
  (value: Filter) => {
    fetch(value);

    const query = setQuery(value);

    let key;
    const { name: routeName, hash: routeHash } = router.currentRoute.value;

    if (routeName === 'member') {
      key = FeatureEventKey.FILTER_MEMBERS;
    } else if (routeName === 'organization') {
      key = FeatureEventKey.FILTER_ORGANIZATIONS;
    } else if (routeName === 'activity' && routeHash === '#activity') {
      key = FeatureEventKey.FILTER_ACTIVITIES;
    } else {
      key = FeatureEventKey.FILTER;
    }

    if (key) {
      trackEvent({
        key,
        type: EventType.FEATURE,
        properties: {
          path: router.currentRoute.value.path,
          filter: value,
        },
      });
    }

    router.replace({ query, hash: props.hash ? `#${props.hash}` : undefined });
  },
  { deep: true },
);

// Watch for query change
const alignQueryUrl = () => {
  const { query } = route;
  const {
    projectGroup, segmentId, menu, ...parsedQuery
  } = query;
  const parsed = parseQuery(
    parsedQuery,
    {
      ...props.config,
      ...props.customConfig,
    },
    props.savedViewsConfig,
  );

  if (!parsed || Object.keys(parsed).length === 0) {
    const query = setQuery(props.modelValue);
    router.replace({ query, hash: props.hash ? `#${props.hash}` : undefined });
    alignFilterList(props.modelValue);
    fetch(props.modelValue);
    return;
  }

  filters.value = parsed as Filter;
  if (!!parsed && Object.keys(parsed).length > 0) {
    alignFilterList(parsed as Filter);
    fetch(parsed as Filter);
  }
};

onMounted(() => {
  alignQueryUrl();
});

const copyToClipboard = async () => {
  const parsedPayload = buildApiFilter(filters.value, { ...props.config, ...props.customConfig }, props.searchConfig, props.savedViewsConfig);

  await navigator.clipboard.writeText(
    JSON.stringify({
      filter: parsedPayload.filter,
      orderBy: parsedPayload.orderBy,
    }),
  );

  ToastStore.success('Filters payload successfully copied to your clipboard');
};

// eslint-disable-next-line vue/define-macros-order -- plain <script> below counts as last, https://eslint.vuejs.org/rules/define-macros-order.html
defineExpose({
  alignFilterList,
});
</script>

<script lang="ts">
export default {
  name: 'LfFilter',
};
</script>
