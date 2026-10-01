<template>
  <el-popover v-model:visible="visible" placement="bottom-start" trigger="click" popper-class="!p-0" width="20rem">
    <template #reference>
      <lf-button type="secondary" size="small">
        <lf-icon name="command" /> <p>Confidence level: <span class="font-normal">{{ label }}</span></p>
      </lf-button>
    </template>

    <div class="pt-1.5 pb-2 px-2">
      <article
        class="px-3 py-2.5 leading-5 font-xs flex justify-between items-center transition cursor-pointer rounded-md hover:bg-gray-50"
        :class="model.length === 0 ? '!bg-primary-50' : ''"
        @click="model = []"
      >
        <span class="text-black">All confidence levels</span>
        <lf-icon v-if="model.length === 0" :size="20" name="check" class="text-primary-600" />
      </article>
    </div>
    <div class="p-2 border-t border-gray-100 flex flex-col gap-1">
      <label
        class="px-3 py-2.5 leading-5 font-xs flex items-center transition cursor-pointer rounded-md hover:bg-gray-50"
      >
        <lf-checkbox v-model="model" value="high" />
        <div class="ml-1 mr-2 h-2 w-2 rounded-full bg-green-600" />
        <p class="text-black text-xs">
          High <span class="text-gray-500">(90% - 100%)</span>
        </p>
      </label>
      <label
        class="px-3 py-2.5 leading-5 font-xs flex items-center transition cursor-pointer rounded-md hover:bg-gray-50"
      >
        <lf-checkbox v-model="model" value="medium" />
        <div class="ml-1 mr-2 h-2 w-2 rounded-full bg-primary-600" />
        <p class="text-black text-xs">
          Medium <span class="text-gray-500">(70% - 89%)</span>
        </p>
      </label>
      <label
        class="px-3 py-2.5 leading-5 font-xs flex items-center transition cursor-pointer rounded-md hover:bg-gray-50"
      >
        <lf-checkbox v-model="model" value="low" />
        <div class="ml-1 mr-2 h-2 w-2 rounded-full bg-yellow-600" />
        <p class="text-black text-xs">
          Low <span class="text-gray-500">(&lt;69%)</span>
        </p>
      </label>
    </div>
    <div class="py-3 px-4 border-t border-gray-100 flex justify-end gap-3">
      <lf-button size="small" type="secondary-ghost" @click="visible = false">
        Cancel
      </lf-button>
      <lf-button size="small" type="primary" :disabled="!hasChanged" @click="apply()">
        Apply
      </lf-button>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import isEqual from 'lodash/isEqual';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { useRouter } from 'vue-router';

const props = defineProps<{
  modelValue: string[]
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string[]): void}>();
const { trackEvent } = useProductTracking();
const router = useRouter();

const visible = ref<boolean>(false);

const model = ref<string[]>(props.modelValue);

const label = computed(() => {
  if (props.modelValue.length > 0) {
    return props.modelValue.map((l) => l.charAt(0).toUpperCase() + l.substring(1)).join(', ');
  }
  return 'All';
});

const apply = () => {
  let key: FeatureEventKey | null = null;
  const { name: routeName } = router.currentRoute.value;

  if (routeName === 'memberMergeSuggestions') {
    key = FeatureEventKey.FILTER_MEMBERS_MERGE_SUGGESTIONS;
  } else if (routeName === 'organizationMergeSuggestions') {
    key = FeatureEventKey.FILTER_ORGANIZATIONS_MERGE_SUGGESTIONS;
  } else {
    key = FeatureEventKey.FILTER;
  }

  if (key) {
    trackEvent({
      key,
      type: EventType.FEATURE,
      properties: {
        filter: {
          confidence: model.value,
        },
      },
    });
  }

  emit('update:modelValue', model.value);
  visible.value = false;
};

const hasChanged = computed(() => !isEqual(model.value, props.modelValue));
</script>

<script lang="ts">
export default {
  name: 'AppMergeSuggestionsConfidenceFilter',
};
</script>
