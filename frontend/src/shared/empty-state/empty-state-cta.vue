<template>
  <div class="flex flex-col items-center justify-center pt-20 pb-10">
    <lf-icon :name="icon" class="text-gray-200" :size="iconSize" />

    <h5 class="mt-3.5">
      {{ title }}
    </h5>
    <div
      v-if="description"
      class="text-gray-600 text-sm mt-4 w-6/12 text-center"
    >
      <lf-icon
        v-if="hasWarningIcon"
        name="circle-info"
        :size="16"
        class="text-primary-500 mr-2"
      />
      <span v-html="description" />
    </div>
    <div class="flex gap-6 mt-8">
      <lf-button
        v-if="ctaBtn"
        type="primary"
        size="medium"
        @click="emit('ctaClick')"
      >
        {{ ctaBtn }}
      </lf-button>
      <lf-button
        v-if="secondaryBtn"
        type="secondary-gray"
        size="medium"
        @click="emit('secondaryClick')"
      >
        {{ secondaryBtn }}
      </lf-button>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

defineProps({
  icon: {
    type: String,
    required: true,
  },
  iconSize: {
    type: Number,
    default: () => 160,
  },
  title: {
    type: String,
    required: true,
  },
  description: {
    type: String,
    default: () => null,
  },
  ctaBtn: {
    type: String,
    default: () => null,
  },
  secondaryBtn: {
    type: String,
    default: () => null,
  },
  hasWarningIcon: {
    type: Boolean,
    default: () => false,
  },
});
const emit = defineEmits(['ctaClick', 'secondaryClick']);
</script>

<style lang="scss">
.empty-list-icon {
  font-size: 160px;
  @apply leading-none text-gray-200;
}
</style>
