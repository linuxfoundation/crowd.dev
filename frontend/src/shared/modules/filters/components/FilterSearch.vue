<template>
  <el-input
    v-model="model"
    clearable
    :placeholder="props.placeholder"
    class="input-with-select !h-9"
    data-qa="filter-search"
    @input="changeValue($event)"
  >
    <template #prefix>
      <lf-icon name="magnifying-glass" class="text-gray-400" :size="16" />
    </template>
    <template v-if="$slots.append" #append>
      <slot name="append" />
    </template>
  </el-input>
</template>

<script setup lang="ts">
import {
  defineProps, ref, watch,
} from 'vue';
import { debounce } from 'lodash';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { useRouter } from 'vue-router';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  modelValue: string,
  placeholder?: string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const model = ref('');

const { trackEvent } = useProductTracking();
const router = useRouter();

watch(() => props.modelValue, (value) => {
  model.value = value;
}, {
  immediate: true,
});

let lastValue = '';

const changeValue = debounce((search: string) => {
  if (search.trim() === lastValue) {
    return;
  }
  lastValue = search.trim();
  let key: FeatureEventKey | null = null;
  const { name: routeName, hash: routeHash } = router.currentRoute.value;

  if (routeName === 'member') {
    key = FeatureEventKey.SEARCH_MEMBERS;
  } else if (routeName === 'organization') {
    key = FeatureEventKey.SEARCH_ORGANIZATIONS;
  } else if (routeName === 'activity' && routeHash === '#activity') {
    key = FeatureEventKey.SEARCH_ACTIVITIES;
  } else {
    key = FeatureEventKey.SEARCH;
  }

  if (key) {
    trackEvent({
      key,
      type: EventType.FEATURE,
    });
  }

  emit('update:modelValue', lastValue);
}, 400);

</script>

<script lang="ts">
export default {
  name: 'LfFilterSearch',
};
</script>
