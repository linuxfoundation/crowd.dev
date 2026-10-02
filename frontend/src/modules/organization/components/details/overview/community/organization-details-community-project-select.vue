<template>
  <lf-dropdown width="20rem" placement="bottom-end" @visibility="onVisibilityChange">
    <template #trigger="{ open }">
      <lf-button type="secondary-link">
        {{ selectedSegmentName }}
        <lf-icon name="chevron-up" :size="16" class="transition" :class="{ 'rotate-180': open }" />
      </lf-button>
    </template>
    <lf-dropdown-item
      :selected="!segment"
      @click="segment = ''"
    >
      All associated project groups
    </lf-dropdown-item>
    <template v-if="getSegments.length > 5">
      <lf-dropdown-separator />
      <div class="-m-2" @click.stop>
        <lf-input
          v-model="search"
          class="h-12 !border-0"
          placeholder="Search project groups..."
        >
          <template #prefix>
            <lf-icon name="magnifying-glass" :size="16" />
          </template>
        </lf-input>
      </div>
    </template>

    <template v-if="getSegments.length > 0">
      <lf-dropdown-separator />
      <div class="max-h-60 overflow-auto">
        <lf-dropdown-item
          v-for="s in filteredSegments"
          :key="s.id"
          :selected="s.id === segment"
          @click="segment = s.id"
        >
          {{ s.name }}
        </lf-dropdown-item>
        <div v-if="filteredSegments.length === 0" class="p-2 text-small text-gray-500 italic">
          No matched project groups
        </div>
      </div>
    </template>
  </lf-dropdown>
</template>

<script setup lang="ts">
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import { getSegmentName } from '@/utils/segments';
import LfDropdownSeparator from '@/ui-kit/dropdown/DropdownSeparator.vue';
import type { Organization } from '@/modules/organization/types/Organization';
import { computed, ref } from 'vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps<{
  organization: Organization,
  segment: string,
}>();

const emit = defineEmits<{(e: 'update:segment', value: string): void }>();

const search = ref<string>('');

const segment = computed({
  get() {
    return props.segment;
  },
  set(val: string) {
    emit('update:segment', val);
  },
});

const getSegments = computed((): { id: string, name: string }[] => props.organization.segments
  .map((segment: string) => ({
    id: segment,
    name: getSegmentName(segment) || '',
  }))
  .filter((s: { id: string, name: string }) => s.name.length > 0));

const filteredSegments = computed(() => getSegments.value
  .filter((s: { id: string, name: string }) => s.name.toLowerCase()
    .includes(search.value.toLowerCase())));

const selectedSegmentName = computed(() => {
  const selectedSegment = getSegments.value.find((s: { id: string, name: string }) => s.id === segment.value);
  return selectedSegment ? selectedSegment.name : 'All associated project groups';
});

const onVisibilityChange = (isVisible: boolean) => {
  if (!isVisible) {
    search.value = '';
  }
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsCommunityProjectSelect',
};
</script>
