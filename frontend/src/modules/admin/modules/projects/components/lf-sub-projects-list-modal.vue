<template>
  <lf-modal
    v-model="model"
    :header-title="title"
    width="30rem"
    content-class="!overflow-unset"
  >
    <div class="px-6">
      <div class="pb-8 relative">
        <app-lf-sub-projects-list-dropdown @on-change="onChange" />
      </div>
    </div>

    <div
      class="bg-gray-50 rounded-b-md flex items-center justify-end py-4 px-6"
    >
      <lf-button
        type="secondary-gray"
        size="medium"
        class="mr-3"
        @click="model = false"
      >
        Cancel
      </lf-button>
      <lf-button
        type="primary"
        size="medium"
        :disabled="!isSubmitEnabled"
        @click="onSubmit"
      >
        Continue
      </lf-button>
    </div>
  </lf-modal>
</template>

<script setup>
import { computed, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import AppLfSubProjectsListDropdown from './lf-sub-projects-list-dropdown.vue';

const props = defineProps({
  modelValue: {
    type: Boolean,
    required: true,
  },
  title: {
    type: String,
    required: true,
  },
});
const emit = defineEmits(['update:modelValue', 'onSubmit']);
const model = computed({
  get() {
    return props.modelValue;
  },
  set(v) {
    emit('update:modelValue', v);
  },
});

const subprojectId = ref('');
const isSubmitEnabled = ref(false);

const onChange = (form) => {
  subprojectId.value = form.subprojectId;
  isSubmitEnabled.value = form.isSubmitEnabled;
};

const onSubmit = () => {
  emit('onSubmit', subprojectId.value);
};
</script>

<script>
export default {
  name: 'AppLfSubProjectsListModal',
};
</script>
