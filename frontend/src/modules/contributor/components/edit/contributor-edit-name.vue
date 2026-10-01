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
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import type { Contributor } from '@/modules/contributor/types/Contributor';

const props = defineProps<{
  contributor: Contributor,
}>();

const { updateContributor } = useContributorStore();

const nameEdit = ref<HTMLDivElement | null>(null);

const form = reactive({
  name: props.contributor.displayName,
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
  if (form.name === props.contributor.displayName) {
    return;
  }
  if ($v.value.$invalid) {
    form.name = props.contributor.displayName;
    return;
  }
  updateContributor(props.contributor.id, {
    displayName: form.name,
  })
    .then(() => {
      ToastStore.success('Name updated successfully!');
    })
    .catch(() => {
      ToastStore.error('There was an error updating contributor');
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfContributorEditName',
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
