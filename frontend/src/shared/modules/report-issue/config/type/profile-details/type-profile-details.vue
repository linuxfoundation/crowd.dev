<template>
  <div class="flex items-center">
    <lf-avatar
      :name="props.entity.displayName"
      :src="avatar(props.entity)"
      :size="24"
      class="!rounded-md border border-gray-200 min-w-6"
      img-class="!object-contain"
    />
    <p class="text-medium pl-2">
      {{ props.entity.displayName }}
    </p>
  </div>
</template>

<script setup lang="ts">
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';

const props = defineProps<{
  attribute: any,
  entity: Contributor,
}>();

const { avatar } = useContributorHelpers();
</script>
