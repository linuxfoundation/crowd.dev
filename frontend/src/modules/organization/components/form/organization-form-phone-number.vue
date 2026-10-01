<template>
  <div>
    <!-- Emails editing -->
    <div>
      <app-organization-form-phone-number-item
        v-for="(_, ei) of model"
        :key="ei"
        v-model="model[ei]"
        class="pb-3"
      >
        <template #actions>
          <lf-button
            type="secondary-ghost"
            size="medium"
            class="w-10 h-10"
            @click="removePhoneNumber(ei)"
          >
            <lf-icon name="trash-can" :size="20" />
          </lf-button>
        </template>
      </app-organization-form-phone-number-item>
      <div class="flex">
        <div class="text-xs font-medium text-primary-500 cursor-pointer" @click="addPhoneNumber()">
          + Add phone number
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import {
  ref, watch,
} from 'vue';
import AppOrganizationFormPhoneNumberItem
  from '@/modules/organization/components/form/organization-form-phone-number-item.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps({
  modelValue: {
    type: Object,
    default: () => {},
  },
});

const emit = defineEmits(['update:modelValue']);

const model = ref([]);

watch(
  props.modelValue,
  (organization, previous) => {
    if (!previous) {
      model.value = organization.phoneNumbers?.length > 0
        ? organization.phoneNumbers
        : [''];
    }
  },
  { deep: true, immediate: true },
);

watch(
  model,
  (value) => {
    // Emit updated organization
    emit('update:modelValue', {
      ...props.modelValue,
      phoneNumbers: value.length ? value : [''],
    });
  },
  { deep: true },
);

const addPhoneNumber = () => {
  model.value.push('');
};
const removePhoneNumber = (index) => {
  if (model.value.length > 1) {
    model.value.splice(index, 1);
  } else if (model.value.length > 0) {
    model.value[0] = '';
  }
};
</script>

<script>
export default {
  name: 'AppOrganizationFormPhoneNumber',
};
</script>
