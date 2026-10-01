<template>
  <div ref="nameEdit">
    <lf-conteneditable
      v-model="form.name"
      class="edit-name px-1 py-px font-secondary text-h5 rounded-md font-semibold transition mb-1
        border border-transparent w-min
        hover:!bg-gray-200 whitespace-nowrap
        focus:!bg-white focus:border-gray-900 focus:!max-w-80 focus:overflow-auto"
      style="max-width: 30ch"
      :class="form.name.length <= 30 ? '' : 'focus:w-full'"
      @blur="update"
      @keydown.enter.prevent
    />
  </div>
</template>

<script setup lang="ts">
import LfConteneditable from '@/ui-kit/contenteditable/Contenteditable.vue';
import { reactive, ref } from 'vue';

import { ToastStore } from '@/shared/message/notification';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import type { Organization } from '@/modules/organization/types/Organization';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';

const props = defineProps<{
  organization: Organization,
}>();

const { updateOrganization } = useOrganizationStore();
const { displayName } = useOrganizationHelpers();

const nameEdit = ref<HTMLDivElement | null>(null);

const form = reactive({
  name: displayName(props.organization),
});

const rules = {
  name: {
    required,
  },
};

const $v = useVuelidate(rules, form);

const update = () => {
  if (nameEdit.value) {
    const scrollElement = nameEdit.value.querySelector('[contenteditable]');
    if (scrollElement) {
      scrollElement.scrollLeft = 0;
    }
  }
  if (form.name === displayName(props.organization)) {
    return;
  }
  if ($v.value.$invalid) {
    form.name = displayName(props.organization);
    return;
  }
  updateOrganization(props.organization.id, {
    attributes: {
      name: {
        default: form.name,
        custom: [form.name],
      },
    },
  })
    .then(() => {
      ToastStore.success('Organization name updated successfully!');
    })
    .catch(() => {
      ToastStore.error('There was an error updating organization');
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationEditName',
};
</script>

<style lang="scss">
.edit-name {
  &:not(:focus){
    @apply truncate;
  }
}

.is-hovered .edit-name {
  @apply bg-gray-100;
}

.edit-name{
  -ms-overflow-style: none;  /* IE and Edge */
  scrollbar-width: none;  /* Firefox */

  &::-webkit-scrollbar {
    display: none;
  }
}
</style>
