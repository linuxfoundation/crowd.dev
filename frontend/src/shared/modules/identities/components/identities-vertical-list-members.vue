<template>
  <div
    v-if="Object.keys({
      ...identities.getIdentities(),
      ...(includeEmails ? { emails: identities.getEmails() } : {}),
    }).length"
    class="flex flex-col gap-3"
  >
    <app-identities-vertical-list
      :identities="{
        ...identities.getIdentities(),
      }"
      :x-padding="xPadding"
      :display-show-more="displayShowMore"
    />
    <app-emails-vertical-list
      v-if="includeEmails"
      :emails="identities.getEmails()"
      :x-padding="xPadding"
      :display-show-more="displayShowMore"
    />
  </div>
  <div v-else class="text-gray-400 mt-6 text-xs italic px-6">
    No identities
  </div>

  <slot :identities="identities" />
</template>

<script setup lang="ts">
import type { Member } from '@/modules/member/types/Member';
import useMemberIdentities from '@/shared/modules/identities/config/useMemberIdentities';
import AppIdentitiesVerticalList from '@/shared/modules/identities/components/identities-vertical-list.vue';
import { computed } from 'vue';
import type { Platform } from '@/shared/modules/platform/types/Platform';
import AppEmailsVerticalList from '@/shared/modules/identities/components/emails-vertical-list.vue';

const props = defineProps<{
  member: Member;
  xPadding?: number;
  displayShowMore?: boolean;
  includeEmails?: boolean;
  order: Platform[]
}>();

const identities = computed(() => useMemberIdentities({
  member: props.member,
  order: props.order,
}));
</script>

<script lang="ts">
export default {
  name: 'AppIdentitiesVerticalListMembers',
};
</script>
