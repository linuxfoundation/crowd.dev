<template>
  <el-input
    v-model="proxy"
    clearable
    :placeholder="props.placeholder"
    class="!h-8"
    @update:model-value="search($event)"
  >
    <template #prefix>
      <lf-icon name="magnifying-glass" :size="16" class="text-gray-400" />
    </template>
  </el-input>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { useRouter } from 'vue-router';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  modelValue: string;
  placeholder?: string;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const { trackEvent } = useProductTracking();
const router = useRouter();

const proxy = ref<string>(props.modelValue);

const search = (val: string) => {
  setTimeout(() => {
    if (proxy.value === val) {
      let key: FeatureEventKey | null = null;
      const { name: routeName } = router.currentRoute.value;

      if (routeName === 'memberMergeSuggestions') {
        key = FeatureEventKey.SEARCH_MEMBERS_MERGE_SUGGESTIONS;
      } else if (routeName === 'organizationMergeSuggestions') {
        key = FeatureEventKey.SEARCH_ORGANIZATIONS_MERGE_SUGGESTIONS;
      } else {
        key = FeatureEventKey.SEARCH;
      }

      if (key) {
        trackEvent({
          key,
          type: EventType.FEATURE,
        });
      }

      emit('update:modelValue', val);
    }
  }, 300);
};
</script>

<script lang="ts">
export default {
  name: 'AppMergeSuggestionsSearch',
};
</script>
