<template>
  <div
    v-if="activeOrganization?.memberOrganizations?.title || member.attributes?.jobTitle?.default"
    class="flex items-start grow mt-2"
  >
    <span class="text-sm text-gray-900 line-clamp-2">
      {{ activeOrganization?.memberOrganizations?.title || member.attributes?.jobTitle?.default }}
    </span>
  </div>
  <div v-else class="text-gray-500 text-xs">
    -
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { Member } from '@/modules/member/types/Member';

defineOptions({ name: 'AppMemberOrganizations' });

const props = defineProps<{
  member: Member;
}>();

const activeOrganization = computed(() => props.member.organizations?.[0]);
</script>
