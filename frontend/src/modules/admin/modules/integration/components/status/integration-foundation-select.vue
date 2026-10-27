<template>
  <lf-dropdown placement="bottom-end" width="20rem">
    <template #trigger>
      <lf-button type="secondary" class="!font-normal">
        <lf-icon name="folders" type="regular" :size="16" />
        {{ selectedFoundation?.name || 'All foundations' }}
        <lf-icon name="chevron-down" :size="16" />
      </lf-button>
    </template>
    <div class="max-h-80 overflow-y-scroll -m-2 p-2">
      <lf-dropdown-item :selected="!model" @click="model = ''">
        All foundations
      </lf-dropdown-item>
      <lf-dropdown-separator />
      <div v-if="isPending" class="py-3 flex justify-center">
        <lf-spinner size="1rem" />
      </div>
      <lf-dropdown-item
        v-for="foundation of foundations"
        v-else
        :key="foundation.id"
        :selected="model === foundation.id"
        @click="model = foundation.id"
      >
        {{ foundation.name }}
      </lf-dropdown-item>
    </div>
  </lf-dropdown>
</template>

<script lang="ts" setup>
import { computed } from 'vue';
import type { QueryFunction } from '@tanstack/vue-query';
import { useQuery } from '@tanstack/vue-query';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import type { Pagination } from '@/shared/types/Pagination';
import { TanstackKey } from '@/shared/types/tanstack';
import { segmentService } from '@/modules/lf/segments/segments.service';
import type { ProjectGroup } from '@/modules/lf/segments/types/Segments';

defineOptions({
  name: 'LfAdminIntegrationFoundationSelect',
});

const props = defineProps<{
  modelValue?: string,
  isLF: boolean | null,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const model = computed({
  get() {
    return props.modelValue || '';
  },
  set(value: string) {
    emit('update:modelValue', value);
  },
});

const { data, isPending } = useQuery<Pagination<ProjectGroup>>({
  queryKey: [TanstackKey.ADMIN_PROJECT_GROUPS, 'integration-foundations'],
  queryFn: segmentService.queryProjectGroups(() => ({
    limit: 500,
    offset: 0,
    filter: {},
  })) as QueryFunction<Pagination<ProjectGroup>>,
});

const foundations = computed<ProjectGroup[]>(() => (data.value?.rows || [])
  .filter((foundation) => props.isLF === null || foundation.isLF === props.isLF)
  .sort((a, b) => a.name.localeCompare(b.name)));

const selectedFoundation = computed(() => (data.value?.rows || []).find((foundation) => foundation.id === model.value));
</script>
