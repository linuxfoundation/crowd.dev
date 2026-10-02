<template>
  <div class="relative">
    <lf-avatar
      :size="48"
      :name="props.contributor.displayName"
      :src="avatar(props.contributor)"
      class="cursor-pointer"
      @click="isEditModalOpen = true"
    >
      <template #overlay>
        <div class="absolute top-0 left-0 w-full h-full bg-secondary-500 opacity-50" />
        <div class="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2">
          <lf-icon name="pen fa-sharp" :size="24" class="text-white" />
        </div>
      </template>
    </lf-avatar>
    <div
      v-if="isNew(props.contributor)"
      class="absolute -top-1.5 left-1/2 border-2 border-white bg-primary-500
      text-xtiny rounded-md px-0.5 text-white font-semibold transform -translate-x-1/2 "
    >
      New
    </div>
  </div>
  <lf-contributor-edit-profile-photo
    v-if="isEditModalOpen"
    v-model="isEditModalOpen"
    :contributor="props.contributor"
  />
</template>

<script setup lang="ts">
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { ref } from 'vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';
import LfContributorEditProfilePhoto from '@/modules/contributor/components/edit/contributor-edit-profile-photo.vue';

const props = defineProps<{
  contributor: Contributor,
}>();

const { isNew, avatar } = useContributorHelpers();

const isEditModalOpen = ref<boolean>(false);
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsHeaderProfilePhoto',
};
</script>
