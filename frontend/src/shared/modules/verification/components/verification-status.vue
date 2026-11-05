<template>
  <lf-popover v-model:visibility="isOpen" trigger-event="hover" :placement="placement" is-dropdown>
    <template #trigger>
      <span
        class="relative flex items-center justify-center h-5 w-5"
        role="button"
        tabindex="0"
        :aria-label="`Verification status: ${title}`"
        @mouseenter="updatePlacement"
        @focus="openOnFocus"
        @blur="isOpen = false"
        @keydown.esc="isOpen = false"
        @keydown.enter.prevent="isOpen = true"
        @keydown.space.prevent="isOpen = true"
      >
        <lf-icon :name="icon" :size="20" :class="iconColor" />
        <lf-icon
          v-if="isStale"
          name="circle"
          type="solid"
          :size="10"
          class="absolute left-3 -top-0.5 text-[#FE9A00]"
        />
      </span>
    </template>

    <div
      class="w-[280px] box-border flex flex-col gap-3 p-[11px] bg-white rounded-lg border border-gray-100
        shadow-[0_1px_2px_-1px_rgba(0,0,0,0.1),0_1px_3px_0_rgba(0,0,0,0.1)]"
    >
      <div class="flex flex-col gap-1">
        <p class="text-2xs leading-[14px] text-neutral-400">
          Verification status
        </p>
        <div class="flex items-center gap-2">
          <lf-icon :name="icon" :size="25" :class="iconColor" />
          <p class="text-sm leading-[14px] font-semibold text-gray-900">
            {{ title }}
          </p>
        </div>
        <p v-if="description" class="text-2xs leading-[18px] text-neutral-500">
          {{ description }}
        </p>
      </div>

      <div class="border-t border-gray-100" />

      <div class="flex flex-col gap-3 text-2xs leading-[18px]">
        <div v-if="status === 'verified'" class="flex items-center justify-between gap-3">
          <span class="text-neutral-500 shrink-0">Verified by:</span>
          <span class="flex items-center gap-1 text-neutral-900 min-w-0">
            <lf-avatar :name="entity.verifiedBy ?? ''" :size="16" />
            <span class="truncate">{{ entity.verifiedBy }}</span>
          </span>
        </div>
        <div v-else-if="status === 'auto-verified'" class="flex items-center justify-between gap-3">
          <span class="text-neutral-500">Verified by:</span>
          <span class="text-neutral-900">LFX platform</span>
        </div>
        <div v-if="status !== 'unverified' && verifiedOn" class="flex items-center justify-between gap-3">
          <span class="text-neutral-500">Verified on:</span>
          <span class="text-neutral-900">{{ formatVerificationDate(verifiedOn) }}</span>
        </div>
        <div v-if="kind === 'work-experience' && source" class="flex items-center justify-between gap-3">
          <span class="text-neutral-500">Source:</span>
          <span class="flex items-center gap-1 text-neutral-900">
            <lf-icon v-if="source.icon" :name="source.icon" :size="20" class="text-neutral-500" />
            <img v-else-if="source.image" :src="source.image" :alt="source.label" class="h-4 w-4">
            {{ source.label }}
          </span>
        </div>
        <div v-if="entity.updatedAt" class="flex items-center justify-between gap-3">
          <span class="text-neutral-500">Last updated:</span>
          <span class="text-neutral-900">{{ formatVerificationDate(entity.updatedAt) }}</span>
        </div>
      </div>

      <template v-if="isStale">
        <div class="border-t border-gray-100" />
        <div class="flex items-start gap-2">
          <lf-icon name="exclamation-circle" type="solid" :size="20" class="shrink-0 text-[#FE9A00]" />
          <p class="text-2xs leading-[18px] text-[#E17100]">
            This identity was updated after it was last verified. It’s worth re-checking.
          </p>
        </div>
      </template>
    </div>
  </lf-popover>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfPopover from '@/ui-kit/popover/Popover.vue';
import {
  formatVerificationDate,
  getVerificationSource,
  getVerificationStatus,
  getVerifiedOn,
  isUpdatedAfterVerification,
  type VerificationEntity,
  type VerificationKind,
} from '@/shared/modules/verification/verification.helpers';

defineOptions({ name: 'LfVerificationStatus' });

const props = defineProps<{
  entity: VerificationEntity,
  kind: VerificationKind,
}>();

const placement = ref<'top' | 'bottom'>('top');

const isOpen = ref(false);

// Popover is up to ~300px tall and LfPopover doesn't flip, so open downward near the viewport top.
const updatePlacement = (e: Event) => {
  placement.value = (e.currentTarget as HTMLElement).getBoundingClientRect().top < 320 ? 'bottom' : 'top';
};

const openOnFocus = (e: FocusEvent) => {
  updatePlacement(e);
  isOpen.value = true;
};

const status = computed(() => getVerificationStatus(props.entity));
const isStale = computed(() => isUpdatedAfterVerification(props.entity, props.kind));
const verifiedOn = computed(() => getVerifiedOn(props.entity));
const source = computed(() => getVerificationSource(props.entity));

const icon = computed(() => (status.value === 'unverified' ? 'circle-dashed' : 'circle-check'));

const iconColor = computed(() => ({
  unverified: 'text-[#90A1B9]',
  verified: 'text-[#00BC7D]',
  'auto-verified': 'text-[#009AFF]',
}[status.value]));

const title = computed(() => ({
  unverified: 'Unverified',
  verified: 'Verified',
  'auto-verified': 'Auto-verified',
}[status.value]));

const description = computed(() => {
  if (status.value === 'auto-verified') {
    return `This ${props.kind === 'work-experience' ? 'work experience' : 'identity'} has been automatically `
      + 'verified by the LFX platform because it came through a connected data source.';
  }
  if (status.value === 'unverified') {
    return props.kind === 'work-experience'
      ? 'This work experience has not yet been verified by our team.'
      : 'This identity has not yet been verified by our team or automatically by the LFX platform.';
  }
  return '';
});

</script>
