<template>
  <div>
    <div class="flex flex-wrap items-end justify-between gap-6 pb-5 pt-6">
      <div class="min-w-0 flex-1">
        <h6 class="mb-1.5 font-semibold text-gray-900">
          Event mapping
        </h6>
        <p class="max-w-2xl text-medium text-gray-600">
          Link each Sessionize event to the CDP project it belongs to. Direct matches are applied
          automatically and don't show here. Confirm or change each best match, and pick a project
          for the unresolved ones. Only confirmed mappings are used when activities are loaded.
        </p>
      </div>
      <div v-if="!loadFailed" class="w-70">
        <div class="mb-1.5 flex justify-between text-xs text-gray-600">
          <span>Reviewed</span>
          <span class="font-semibold text-gray-900">{{ reviewed }} of {{ total }}</span>
        </div>
        <div class="h-2 overflow-hidden rounded bg-neutral-100">
          <div class="h-full rounded bg-primary-500" :style="{ width: `${progress}%` }" />
        </div>
      </div>
    </div>

    <div v-if="loadFailed" class="flex flex-col items-center gap-3 pt-14">
      <p class="text-small text-gray-500">
        Could not load event mappings.
      </p>
      <lf-button type="secondary" @click="refetch()">
        Retry
      </lf-button>
    </div>
    <div v-else class="flex flex-col items-start gap-4 pb-4">
      <lf-dropdown placement="bottom-start" width="14rem">
        <template #trigger>
          <lf-button type="secondary">
            <span
              v-if="selectedPlatform.dotClass"
              class="h-2 w-2 rounded-full"
              :class="selectedPlatform.dotClass"
            />
            {{ selectedPlatform.label }}
            <lf-icon name="chevron-down" :size="16" />
          </lf-button>
        </template>
        <lf-dropdown-item
          v-for="option of platformOptions"
          :key="option.value"
          :selected="platform === option.value"
          @click="platform = option.value"
        >
          <div class="flex items-center gap-2">
            <span v-if="option.dotClass" class="h-2 w-2 rounded-full" :class="option.dotClass" />
            {{ option.label }}
          </div>
        </lf-dropdown-item>
      </lf-dropdown>

      <lf-tabs v-model="status" :fragment="false">
        <lf-tab v-for="tab of statusTabs" :key="tab.state" :name="tab.state">
          <div class="flex items-center gap-1.5">
            <span>{{ tab.label }}</span>
            <lf-pill :type="tab.state === status ? 'primary' : 'secondary'">
              {{ counts[tab.state] }}
            </lf-pill>
          </div>
        </lf-tab>
      </lf-tabs>
    </div>

    <div
      v-if="!loadFailed && selected.size > 0"
      class="mb-3 flex flex-wrap items-center gap-3 rounded-lg border border-primary-100 bg-primary-25 px-3.5 py-2.5 text-medium"
    >
      <span class="font-semibold">{{ selected.size }} selected</span>
      <span class="flex-1" />
      <lf-button type="secondary-ghost" @click="clearSelection()">
        Clear
      </lf-button>
      <lf-button :disabled="isMutating" @click="confirmSelected()">
        Confirm selected
      </lf-button>
    </div>

    <div v-if="isPending" class="flex justify-center pt-8">
      <lf-spinner />
    </div>
    <div v-else-if="visibleRows.length > 0" class="overflow-x-auto">
      <lf-table class="min-w-[1040px]">
        <thead>
          <tr>
            <lf-table-head class="w-8" />
            <lf-table-head class="w-1/4">
              Event
            </lf-table-head>
            <lf-table-head class="w-28">
              Platform
            </lf-table-head>
            <lf-table-head class="w-1/5">
              Match
            </lf-table-head>
            <lf-table-head class="w-1/5">
              Project
            </lf-table-head>
            <lf-table-head class="w-48">
              <div class="flex w-full justify-end">
                Actions
              </div>
            </lf-table-head>
          </tr>
        </thead>
        <tbody>
          <lf-event-mapping-row
            v-for="row of visibleRows"
            :key="row.eventKey"
            :row="row"
            :picks="picks"
            :decided-now="decidedNow"
            :selected="selected.has(row.eventKey)"
            :picker-open="openPicker === row.eventKey"
            :busy="isMutating"
            @update:selected="setSelected(row.eventKey, $event)"
            @update:picker-open="setPickerOpen(row.eventKey, $event)"
            @pick="pick(row, $event)"
            @confirm="confirmRow(row)"
            @ignore="ignoreMutation.mutate(reference(row))"
            @restore="restoreMutation.mutate(reference(row))"
          />
        </tbody>
      </lf-table>
    </div>
    <div v-else-if="!loadFailed" class="flex flex-col items-center pt-14">
      <lf-icon name="eyes" :size="64" class="text-gray-300" />
      <h6 class="pt-6 text-center font-semibold text-gray-900">
        Nothing here
      </h6>
      <p class="pt-3 text-center text-small text-gray-500">
        Try another platform or status.
      </p>
    </div>

    <p class="mt-5 text-tiny text-gray-500">
      Best match: a similar name within 3 days of a Cvent event with a clear lead, one name contained
      in the other, or a sub-event mapped to its main event.
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { isAxiosError } from 'axios';
import { useMutation } from '@tanstack/vue-query';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfPill from '@/ui-kit/pill/Pill.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import LfTabs from '@/ui-kit/tabs/Tabs.vue';
import LfTab from '@/ui-kit/tabs/Tab.vue';
import { ToastStore } from '@/shared/message/notification';
import { useInvalidate } from '@/shared/queries/invalidate';
import LfEventMappingRow from '@/modules/admin/modules/event-mapping/components/event-mapping-row.vue';
import { eventMappingPlatforms } from '@/modules/admin/modules/event-mapping/config/platforms';
import {
  confirmItem,
  confirmItems,
  keepDecidedNow,
  keepKnownPicks,
  keepSelectable,
  pickProject,
  reviewedCount,
  rowsInTab,
  statusCounts,
} from '@/modules/admin/modules/event-mapping/event-mapping-helper';
import { useEventMappingListQuery } from '@/modules/admin/modules/event-mapping/queries/useEventMappingListQuery';
import {
  confirmEventMappings,
  ignoreEventMapping,
  restoreEventMapping,
} from '@/modules/admin/modules/event-mapping/services/event-mapping.api.service';
import type {
  EventMappingConfirmItem,
  EventMappingPlatform,
  EventMappingPlatformFilter,
  EventMappingPlatformOption,
  EventMappingProject,
  EventMappingReference,
  EventMappingRow,
  EventMappingState,
  EventMappingStatusTab,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';

defineOptions({
  name: 'LfEventMappingPage',
});

const statusTabs: EventMappingStatusTab[] = [
  { state: 'suggested', label: 'Best match' },
  { state: 'unresolved', label: 'Unresolved' },
  { state: 'confirmed', label: 'Confirmed' },
  { state: 'ignored', label: 'Ignored' },
];

const platformOptions: EventMappingPlatformOption[] = [
  { value: 'all', label: 'All platforms' },
  ...(Object.keys(eventMappingPlatforms) as EventMappingPlatform[]).map((value) => ({
    value,
    ...eventMappingPlatforms[value],
  })),
];

const invalidate = useInvalidate();
const {
  data, isPending, error, refetch,
} = useEventMappingListQuery(ref(true));

const platform = ref<EventMappingPlatformFilter>('all');
const status = ref<EventMappingState>('suggested');
const picks = ref<Map<string, EventMappingProject>>(new Map());
const decidedNow = ref<Map<string, string | null>>(new Map());
const selected = ref<Set<string>>(new Set());
const openPicker = ref<string | null>(null);

const loadFailed = computed(() => !!error.value && !data.value);
const rows = computed<EventMappingRow[]>(() => data.value?.rows ?? []);
const counts = computed(() => statusCounts(rows.value, platform.value));
const visibleRows = computed(() => rowsInTab(rows.value, platform.value, status.value));
const reviewed = computed(() => reviewedCount(counts.value));
const total = computed(() => Object.values(counts.value).reduce((sum, count) => sum + count, 0));
const progress = computed(() => (total.value ? Math.round((reviewed.value / total.value) * 100) : 0));
const selectedPlatform = computed(
  () => platformOptions.find(({ value }) => value === platform.value) ?? platformOptions[0],
);

const clearSelection = () => {
  selected.value = new Set();
};

const setSelected = (eventKey: string, isSelected: boolean) => {
  const next = new Set(selected.value);
  if (isSelected) {
    next.add(eventKey);
  } else {
    next.delete(eventKey);
  }
  selected.value = next;
};

const setPickerOpen = (eventKey: string, open: boolean) => {
  if (open) {
    openPicker.value = eventKey;
  } else if (openPicker.value === eventKey) {
    openPicker.value = null;
  }
};

const pick = (row: EventMappingRow, project: EventMappingProject) => {
  picks.value = pickProject(picks.value, row, project);
};

const markDecided = (eventKeys: string[]) => {
  const next = new Map(decidedNow.value);
  eventKeys.forEach((eventKey) => next.set(eventKey, null));
  decidedNow.value = next;
};

const forgetDecided = (eventKey: string) => {
  const next = new Map(decidedNow.value);
  next.delete(eventKey);
  decidedNow.value = next;
};

const reference = ({ platform: rowPlatform, eventKey }: EventMappingRow): EventMappingReference => ({
  platform: rowPlatform,
  eventKey,
});

const serverMessage = (err: unknown): string => {
  if (isAxiosError(err) && typeof err.response?.data === 'string' && err.response.data) {
    return err.response.data;
  }
  return 'Something went wrong';
};

const onError = (err: unknown) => {
  ToastStore.error(serverMessage(err));
  return invalidate.eventMapping();
};

const confirmMutation = useMutation({
  mutationFn: (items: EventMappingConfirmItem[]) => confirmEventMappings(items),
  onSuccess: (_, items) => {
    markDecided(items.map(({ eventKey }) => eventKey));
    return invalidate.eventMapping();
  },
  onError,
});
const ignoreMutation = useMutation({
  mutationFn: ignoreEventMapping,
  onSuccess: (_, { eventKey }) => {
    markDecided([eventKey]);
    return invalidate.eventMapping();
  },
  onError,
});
const restoreMutation = useMutation({
  mutationFn: restoreEventMapping,
  onSuccess: (_, { eventKey }) => {
    forgetDecided(eventKey);
    return invalidate.eventMapping();
  },
  onError,
});

const isMutating = computed(
  () => confirmMutation.isPending.value
    || ignoreMutation.isPending.value
    || restoreMutation.isPending.value,
);

const confirmRow = (row: EventMappingRow) => {
  const item = confirmItem(row, picks.value);
  if (item) {
    confirmMutation.mutate([item]);
  }
};

const confirmSelected = () => {
  confirmMutation.mutate(confirmItems(visibleRows.value, selected.value, picks.value));
};

watch([platform, status], () => {
  clearSelection();
  openPicker.value = null;
});

watch(rows, (current) => {
  picks.value = keepKnownPicks(picks.value, current);
  decidedNow.value = keepDecidedNow(decidedNow.value, current);
});

watch([visibleRows, picks], ([current, currentPicks]) => {
  selected.value = keepSelectable(selected.value, current, currentPicks);
});

watch(error, (err) => {
  if (err) {
    ToastStore.error('Something went wrong while fetching event mappings');
  }
});
</script>
