<template>
  <lf-modal v-model="isModalOpen">
    <template #default>
      <div class="px-6 pt-8 flex gap-4">
        <div class="min-w-10 w-10 h-10 rounded-full bg-red-50 flex items-center justify-center">
          <lf-icon name="circle-exclamation" class="text-red-500" :size="16" />
        </div>
        <div class="flex flex-col gap-6">
          <div class="flex flex-col gap-2">
            <span class="font-semibold">Are you sure you want to leave?</span>
            <p class="text-gray-500 text-small">
              You have unsaved changes that will be lost if you leave now. You can save your changes or continue editing.
            </p>
          </div>
        </div>
      </div>
      <div class="px-6 py-4.5 bg-gray-50 mt-8 flex justify-end gap-4">
        <lf-button type="outline" @click="keepEditing">
          Keep editing
        </lf-button>
        <lf-button type="danger" class="!rounded-full" @click="discardChanges">
          Discard changes
        </lf-button>
      </div>
    </template>
  </lf-modal>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const isModalOpen = ref(false);

let resolvePromise: ((value: boolean) => void) | null = null;

const open = (): Promise<boolean> => {
  isModalOpen.value = true;
  return new Promise((resolve) => {
    resolvePromise = resolve;
  });
};

const keepEditing = () => {
  isModalOpen.value = false;
  if (resolvePromise) {
    resolvePromise(false);
    resolvePromise = null;
  }
};

const discardChanges = () => {
  isModalOpen.value = false;
  if (resolvePromise) {
    resolvePromise(true);
    resolvePromise = null;
  }
};

defineExpose({
  open,
});
</script>

<script lang="ts">
export default {
  name: 'ChangesConfirmationModal',
};
</script>
