<template>
  <lf-modal v-model="model" width="42rem">
    <div class="flex flex-col">
      <!-- Header -->
      <section class="p-6 flex">
        <span
          class="rounded-full bg-red-100 w-10 h-10 mr-4 flex items-center justify-center"
        >
          <lf-icon :name="icon" :size="24" class="text-red-500" />
        </span>
        <div class="pt-2 flex flex-col">
          <span class="text-black text-base font-semibold">
            {{ title }}
          </span>
          <span class="pt-2 text-gray-500 text-sm">
            {{ description }}
          </span>
        </div>
      </section>
      <!-- Body -->
      <section class="p-6 pt-0 ml-14">
        <p class="text-gray-900 text-xs font-semibold">
          Type {{ confirmText.toUpperCase() }} to confirm
        </p>
        <lf-input
          v-model="removeConfirm"
          :placeholder="`Type ${confirmText.toUpperCase()} to confirm`"
        />
      </section>
      <!-- Footer -->
      <section class="px-6 py-4.5 bg-gray-50 flex justify-end">
        <lf-button type="secondary-ghost" @click="onClose">
          {{ cancelButtonText }}
        </lf-button>
        <lf-button
          type="danger"
          :disabled="removeConfirm.toLowerCase() !== confirmText.toLowerCase()"
          @click="onConfirm"
        >
          {{ confirmButtonText }}
        </lf-button>
      </section>
    </div>
  </lf-modal>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';

defineOptions({
  name: 'DeleteConfirmDialog',
});

const props = defineProps<{
  modelValue: boolean;
  title: string;
  description: string;
  confirmText: string;
  confirmButtonText: string;
  cancelButtonText: string;
  icon: string;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'confirm'): void;
  (e: 'close'): void;
}>();

const removeConfirm = ref('');

const model = computed({
  get() {
    return props.modelValue;
  },
  set(v) {
    emit('update:modelValue', v);
  },
});

const onClose = () => {
  emit('close');
};

const onConfirm = () => {
  emit('confirm');
};
</script>
