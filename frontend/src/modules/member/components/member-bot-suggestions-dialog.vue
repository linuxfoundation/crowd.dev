<template>
  <lf-modal
    v-if="isModalOpen"
    v-model="isModalOpen"
    container-class="overflow-auto"
    content-class="!max-h-none"
    width="36rem"
  >
    <app-member-bot-suggestions
      :offset="props.offset"
      class="!border-t-0 !shadow-none"
      @reload="emit('reload')"
    >
      <template #actions>
        <lf-button
          type="secondary-ghost-light"
          :icon-only="true"
          @click="isModalOpen = false"
        >
          <lf-icon name="xmark" :size="16" />
        </lf-button>
      </template>
    </app-member-bot-suggestions>
  </lf-modal>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import AppMemberBotSuggestions from '@/modules/member/components/member-bot-suggestions.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';

const props = withDefaults(
  defineProps<{
    modelValue: boolean;
    offset?: number;
  }>(),
  {
    offset: 0,
  },
);

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'reload'): void;
}>();

const isModalOpen = computed<boolean>({
  get() {
    return props.modelValue;
  },
  set() {
    emit('update:modelValue', false);
  },
});
</script>

<script lang="ts">
export default {
  name: 'AppMemberBotSuggestionsDialog',
};
</script>
