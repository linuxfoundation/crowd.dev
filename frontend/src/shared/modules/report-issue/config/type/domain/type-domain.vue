<template>
  <div class="flex items-center">
    <lf-icon name="link" :size="20" class="text-gray-500" />
    <p class="text-medium pl-2">
      {{ props.attribute.value }}
    </p>
  </div>
</template>

<script setup lang="ts">
import type { ContributorIdentity } from '@/modules/contributor/types/Contributor';
import type { OrganizationIdentity } from '@/modules/organization/types/Organization';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  attribute: ContributorIdentity | OrganizationIdentity
}>();
</script>
