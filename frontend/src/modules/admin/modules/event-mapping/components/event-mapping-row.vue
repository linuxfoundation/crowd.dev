<template>
  <tr>
    <td class="w-8 pl-2 align-top">
      <lf-checkbox
        v-if="selectable"
        class="!m-0"
        :model-value="props.selected"
        @update:model-value="emit('update:selected', $event === true)"
      />
    </td>
    <td class="align-top">
      <p class="text-medium font-semibold leading-snug text-gray-900">
        {{ props.row.eventName }}
      </p>
      <p class="mt-1 text-tiny text-gray-500">
        {{ eventDates }}
      </p>
    </td>
    <td class="w-28 align-top">
      <lf-pill type="secondary" class="w-fit gap-1.5 !bg-gray-100 !text-gray-700">
        <span class="h-2 w-2 rounded-full" :class="platform.dotClass" />
        {{ platform.label }}
      </lf-pill>
    </td>
    <td class="w-1/5 align-top">
      <lf-pill type="transparent" class="w-fit" :class="badge.colorClass">
        {{ badge.label }}
      </lf-pill>
      <p class="mt-1.5 text-tiny text-gray-600">
        {{ evidence }}
      </p>
      <lf-button
        v-if="hintProject"
        type="primary-ghost"
        size="tiny"
        class="mt-1.5 !border-dashed !border-primary-200"
        :disabled="props.busy"
        @click="emit('pick', hintProject)"
      >
        Use {{ hintProject.name }}
      </lf-button>
    </td>
    <td class="w-1/5 align-top">
      <div v-if="project" class="flex items-start gap-2">
        <lf-icon name="layer-group" :size="16" class="mt-0.5 text-gray-700" />
        <div class="min-w-0">
          <p class="text-medium font-semibold text-gray-900">
            {{ project.name }}
          </p>
          <p class="mt-0.5 text-tiny text-gray-500">
            {{ project.groupName }}
          </p>
        </div>
      </div>
      <lf-event-mapping-project-picker
        v-if="pending"
        :open="props.pickerOpen"
        :current-segment-id="project?.segmentId"
        @update:open="emit('update:pickerOpen', $event)"
        @pick="emit('pick', $event)"
      >
        <template #trigger>
          <lf-button
            v-if="project"
            type="primary-link"
            size="small"
            class="mt-1.5 !gap-1 !px-0"
            :disabled="props.busy"
          >
            Change project
            <lf-icon name="chevron-down" :size="14" />
          </lf-button>
          <lf-button
            v-else
            type="secondary"
            class="w-56 !justify-between !font-normal !text-gray-600"
            :disabled="props.busy"
          >
            Select project
            <lf-icon name="chevron-down" :size="14" />
          </lf-button>
        </template>
      </lf-event-mapping-project-picker>
    </td>
    <td class="w-48 align-top">
      <div v-if="pending" class="flex flex-wrap justify-end gap-1.5">
        <lf-button
          type="secondary"
          size="small"
          :disabled="props.busy"
          @click="emit('ignore')"
        >
          Ignore
        </lf-button>
        <lf-button
          type="primary"
          size="small"
          :disabled="props.busy || !project"
          @click="emit('confirm')"
        >
          Confirm
        </lf-button>
      </div>
      <div v-else class="text-right">
        <p
          v-if="props.row.state === 'confirmed'"
          class="inline-flex items-center gap-1.5 text-xs font-semibold text-green-800"
        >
          <lf-icon name="check" type="regular" :size="14" />
          Confirmed
        </p>
        <p v-else class="text-xs font-semibold text-gray-600">
          Ignored
        </p>
        <p v-if="decisionDate" class="mt-0.5 text-tiny text-gray-500">
          {{ decisionDate }}
        </p>
        <lf-button
          v-if="props.row.state === 'ignored' || props.row.canUndo"
          type="primary-link"
          size="tiny"
          class="!px-0"
          :disabled="props.busy"
          @click="emit('restore')"
        >
          {{ props.row.state === 'ignored' ? 'Restore' : 'Undo' }}
        </lf-button>
        <p v-else class="py-1 text-tiny text-gray-500">
          Loading started
        </p>
      </div>
    </td>
  </tr>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfPill from '@/ui-kit/pill/Pill.vue';
import LfEventMappingProjectPicker from '@/modules/admin/modules/event-mapping/components/event-mapping-project-picker.vue';
import { eventMappingPlatforms } from '@/modules/admin/modules/event-mapping/config/platforms';
import type { PickedProjects } from '@/modules/admin/modules/event-mapping/event-mapping-helper';
import {
  displayedProject,
  formatDecisionDate,
  formatEventDates,
  isPending,
  isSelectable,
  rowBadge,
  rowEvidence,
  showsHint,
} from '@/modules/admin/modules/event-mapping/event-mapping-helper';
import type {
  EventMappingProject,
  EventMappingRow,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';

defineOptions({
  name: 'LfEventMappingRow',
});

const props = defineProps<{
  row: EventMappingRow;
  picks: PickedProjects;
  selected: boolean;
  pickerOpen: boolean;
  busy: boolean;
}>();

const emit = defineEmits<{(e: 'update:selected', value: boolean): void;
  (e: 'update:pickerOpen', value: boolean): void;
  (e: 'pick', value: EventMappingProject): void;
  (e: 'confirm'): void;
  (e: 'ignore'): void;
  (e: 'restore'): void;
}>();

const pending = computed(() => isPending(props.row));
const selectable = computed(() => isSelectable(props.row, props.picks));
const project = computed(() => displayedProject(props.row, props.picks));
const badge = computed(() => rowBadge(props.row, props.picks));
const evidence = computed(() => rowEvidence(props.row, props.picks));
const hintProject = computed(() => (showsHint(props.row, props.picks) ? props.row.hintProject : null));
const platform = computed(() => eventMappingPlatforms[props.row.platform]);
const eventDates = computed(() => formatEventDates(props.row.eventStart, props.row.eventEnd));
const decisionDate = computed(() => (props.row.decidedAt ? formatDecisionDate(props.row.decidedAt) : ''));
</script>
