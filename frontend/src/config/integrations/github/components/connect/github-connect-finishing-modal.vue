<template>
  <lf-modal v-model="isModalOpen" width="25rem">
    <div class="p-6">
      <div class="flex items-center gap-2">
        <h5>Finishing the setup</h5>
        <lf-spinner size="1.25rem" />
      </div>
      <p class="text-small pt-3 text-gray-500">
        We're finishing the last steps of the
        <span class="font-semibold">GitHub</span> <br />
        integration setup, please don't reload the page.
      </p>
    </div>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import { computed } from 'vue';

defineOptions({
  name: 'LfGithubConnectFinishingModal',
});

const props = defineProps<{
  modelValue: boolean;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void }>();

const isModalOpen = computed<boolean>({
  get: () => props.modelValue,
  set: (value) => emit('update:modelValue', value),
});
</script>
