<template>
  <lf-dropdown placement="bottom-end" width="20rem">
    <template #trigger>
      <lf-button type="secondary" class="!font-normal">
        <template v-if="!model">
          <lf-icon name="grid-round-2" type="regular" :size="16" />
          All integrations
        </template>
        <template v-else>
          <img :src="lfIntegrations()[model]?.image" :alt="lfIntegrations()[model]?.name" class="min-w-4 h-4 object-contain">
          {{ lfIntegrations()[model]?.name }}
        </template>
        <lf-icon name="chevron-down" :size="16" />
      </lf-button>
    </template>
    <div class="max-h-80 overflow-y-scroll -m-2 p-2">
      <lf-dropdown-item :selected="!model" @click="model = ''">
        <div class="flex items-center gap-2">
          <lf-icon name="grid-round-2" type="regular" :size="16" />
          All integrations
        </div>
      </lf-dropdown-item>
      <lf-dropdown-separator />

      <lf-dropdown-item
        v-for="(integration, key) in connectableIntegrations"
        :key="key"
        :selected="model === key"
        @click="model = key"
      >
        <div class="flex items-center gap-2">
          <img :src="integration.image" :alt="integration.name" class="min-w-4 h-4 object-contain">
          {{ integration.name }}
        </div>
      </lf-dropdown-item>
    </div>
  </lf-dropdown>
</template>

<script lang="ts" setup>
import { computed } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import { lfIntegrations } from '@/config/integrations';

const props = defineProps<{
  modelValue?: string,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const adminNotConnectablePlatforms = ['twitter'];

const connectableIntegrations = computed(() => Object.fromEntries(
  Object.entries(lfIntegrations())
    .filter(([key, config]) => !config.autoManaged && !adminNotConnectablePlatforms.includes(key)),
));

const model = computed({
  get() {
    return props.modelValue || '';
  },
  set(value: string) {
    emit('update:modelValue', value);
  },
});
</script>

<script lang="ts">
export default {
  name: 'LfAdminIntegrationPlatformSelect',
};
</script>
