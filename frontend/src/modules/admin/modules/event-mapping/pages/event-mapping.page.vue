<template>
  <div>
    <div class="flex flex-wrap items-end justify-between gap-6 pb-5 pt-6">
      <div class="min-w-0 flex-1">
        <h2 class="mb-1.5 font-primary text-[18px] font-bold text-gray-900">
          Event mapping
        </h2>
        <p class="max-w-2xl text-medium text-gray-600">
          Link each Sessionize event to the CDP project it belongs to. Direct matches are applied
          automatically and don't show here. Confirm or change each best match, and pick a project
          for the unresolved ones. Only confirmed mappings are used when activities are loaded.
        </p>
      </div>
      <div class="w-70">
        <div class="mb-1.5 flex justify-between text-xs text-gray-600">
          <span>Reviewed</span>
          <span class="font-semibold text-gray-900">{{ reviewed }} of {{ total }}</span>
        </div>
        <div class="h-2 overflow-hidden rounded bg-neutral-100">
          <div class="h-full rounded bg-primary-500" :style="{ width: `${progress}%` }" />
        </div>
      </div>
    </div>

    <div class="pb-4">
      <lf-tabs
        v-model="platform"
        :fragment="false"
        class="rounded-lg border border-gray-200 p-1 !gap-1"
      >
        <lf-tab v-for="option of platformOptions" :key="option.value" :name="option.value">
          <span v-if="option.dotClass" class="mr-2 h-2 w-2 rounded-full" :class="option.dotClass" />
          {{ option.label }}
        </lf-tab>
      </lf-tabs>
    </div>

    <el-tabs v-model="status" class="event-mapping-status-tabs">
      <el-tab-pane v-for="tab of statusTabs" :key="tab.state" :name="tab.state">
        <template #label>
          <span class="flex items-center gap-2">
            {{ tab.label }}
            <lf-pill
              type="secondary"
              :class="tab.state === status
                ? '!bg-primary-500 !text-white'
                : '!bg-gray-100 !text-gray-700'"
            >
              {{ counts[tab.state] }}
            </lf-pill>
          </span>
        </template>
      </el-tab-pane>
    </el-tabs>

    <div
      v-if="selected.size > 0"
      class="mt-3 flex flex-wrap items-center gap-3 rounded-lg border border-primary-100 bg-primary-25 px-3.5 py-2.5 text-medium"
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
    <template v-else>
      <lf-table class="mt-1">
        <thead>
          <tr>
            <lf-table-head class="w-8" />
            <lf-table-head>Event</lf-table-head>
            <lf-table-head class="w-28">
              Platform
            </lf-table-head>
            <lf-table-head class="w-1/5">
              Match
            </lf-table-head>
            <lf-table-head class="w-1/5">
              Project
            </lf-table-head>
            <th class="w-48 !text-right">
              Actions
            </th>
          </tr>
        </thead>
        <tbody>
          <lf-event-mapping-row
            v-for="row of visibleRows"
            :key="row.eventKey"
            :row="row"
            :picks="picks"
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
      <p v-if="visibleRows.length === 0" class="px-2 py-12 text-center text-medium text-gray-500">
        Nothing here. Try another status.
      </p>
    </template>

    <p class="mt-5 text-tiny text-gray-500">
      Best match: a similar name within 3 days of a Cvent event with a clear lead, one name contained
      in the other, or a sub-event mapped to its main event.
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { isAxiosError } from 'axios';
import { useMutation, useQueryClient } from '@tanstack/vue-query';
import LfButton from '@/ui-kit/button/Button.vue';
import LfPill from '@/ui-kit/pill/Pill.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import LfTabs from '@/ui-kit/tabs/Tabs.vue';
import LfTab from '@/ui-kit/tabs/Tab.vue';
import { ToastStore } from '@/shared/message/notification';
import { useInvalidate } from '@/shared/queries/invalidate';
import { queryKeys } from '@/shared/queries/keys';
import LfEventMappingRow from '@/modules/admin/modules/event-mapping/components/event-mapping-row.vue';
import { eventMappingPlatforms } from '@/modules/admin/modules/event-mapping/config/platforms';
import type {
  EventMappingPlatformFilter,
} from '@/modules/admin/modules/event-mapping/event-mapping-helper';
import {
  applyConfirmed,
  applyIgnored,
  applyRestored,
  confirmation,
  confirmations,
  keepPendingPicks,
  keepSelectable,
  pickProject,
  reviewedCount,
  rowsInTab,
  statusCounts,
  toConfirmItems,
} from '@/modules/admin/modules/event-mapping/event-mapping-helper';
import { useEventMappingListQuery } from '@/modules/admin/modules/event-mapping/queries/useEventMappingListQuery';
import {
  confirmEventMappings,
  ignoreEventMapping,
  restoreEventMapping,
} from '@/modules/admin/modules/event-mapping/services/event-mapping.api.service';
import type {
  EventMappingConfirmation,
  EventMappingList,
  EventMappingPlatform,
  EventMappingProject,
  EventMappingReference,
  EventMappingRow,
  EventMappingState,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';

defineOptions({
  name: 'LfEventMappingPage',
});

const statusTabs: { state: EventMappingState; label: string }[] = [
  { state: 'suggested', label: 'Best match' },
  { state: 'unresolved', label: 'Unresolved' },
  { state: 'confirmed', label: 'Confirmed' },
  { state: 'ignored', label: 'Ignored' },
];

const platformOptions: { value: EventMappingPlatformFilter; label: string; dotClass?: string }[] = [
  { value: 'all', label: 'All platforms' },
  ...(Object.keys(eventMappingPlatforms) as EventMappingPlatform[]).map((value) => ({
    value,
    ...eventMappingPlatforms[value],
  })),
];

const REFRESH_DELAY_MS = 5000;

const invalidate = useInvalidate();
const queryClient = useQueryClient();
const { data, isPending, error } = useEventMappingListQuery(ref(true));

const platform = ref<EventMappingPlatformFilter>('all');
const status = ref<EventMappingState>('suggested');
const picks = ref<Map<string, EventMappingProject>>(new Map());
const selected = ref<Set<string>>(new Set());
const openPicker = ref<string | null>(null);

const rows = computed<EventMappingRow[]>(() => data.value?.rows ?? []);
const counts = computed(() => statusCounts(rows.value, platform.value));
const visibleRows = computed(() => rowsInTab(rows.value, platform.value, status.value));
const reviewed = computed(() => reviewedCount(counts.value));
const total = computed(() => Object.values(counts.value).reduce((sum, count) => sum + count, 0));
const progress = computed(() => (total.value ? Math.round((reviewed.value / total.value) * 100) : 0));

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

let refreshTimer: ReturnType<typeof setTimeout> | undefined;

const scheduleRefresh = () => {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(() => invalidate.eventMapping(), REFRESH_DELAY_MS);
};

const applyDecision = async (update: (list: EventMappingList) => EventMappingList) => {
  await queryClient.cancelQueries({ queryKey: queryKeys.eventMapping.all });
  queryClient.setQueryData<EventMappingList>(
    queryKeys.eventMapping.all,
    (list) => (list ? update(list) : list),
  );
  scheduleRefresh();
};

const onDecisionError = (err: unknown) => {
  ToastStore.error(serverMessage(err));
  clearTimeout(refreshTimer);
  return invalidate.eventMapping();
};

const now = () => new Date().toISOString();

const confirmMutation = useMutation({
  mutationFn: (decisions: EventMappingConfirmation[]) => confirmEventMappings(toConfirmItems(decisions)),
  onSuccess: (_, decisions) => applyDecision((list) => applyConfirmed(list, decisions, now())),
  onError: onDecisionError,
});
const ignoreMutation = useMutation({
  mutationFn: ignoreEventMapping,
  onSuccess: (_, { eventKey }) => applyDecision((list) => applyIgnored(list, eventKey, now())),
  onError: onDecisionError,
});
const restoreMutation = useMutation({
  mutationFn: restoreEventMapping,
  onSuccess: (_, { eventKey }) => applyDecision((list) => applyRestored(list, eventKey)),
  onError: onDecisionError,
});

const isMutating = computed(
  () => confirmMutation.isPending.value
    || ignoreMutation.isPending.value
    || restoreMutation.isPending.value,
);

const confirmRow = (row: EventMappingRow) => {
  const decision = confirmation(row, picks.value);
  if (decision) {
    confirmMutation.mutate([decision]);
  }
};

const confirmSelected = () => {
  confirmMutation.mutate(confirmations(visibleRows.value, selected.value, picks.value));
};

watch([platform, status], () => {
  clearSelection();
  openPicker.value = null;
});

watch(rows, (current) => {
  picks.value = keepPendingPicks(picks.value, current);
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

<style scoped>
.event-mapping-status-tabs :deep(.el-tabs__nav-wrap::after) {
  @apply h-px bg-gray-200;
}

.event-mapping-status-tabs :deep(.el-tabs__item) {
  @apply px-0 mr-6 text-gray-600;
}

.event-mapping-status-tabs :deep(.el-tabs__item.is-active) {
  @apply font-semibold text-gray-900 !important;
}
</style>
