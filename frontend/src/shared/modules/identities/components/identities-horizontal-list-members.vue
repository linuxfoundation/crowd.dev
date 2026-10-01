<template>
  <app-identities-horizontal-list
    :identities="identities.getIdentities()"
    :limit="limit"
    :as-svg="asSvg"
    v-bind="$attrs"
  >
    <template v-if="$slots.badge" #badge>
      <slot name="badge" />
    </template>
  </app-identities-horizontal-list>
</template>

<script setup lang="ts">
import { defineProps, computed } from 'vue';
import useMemberIdentities from '@/shared/modules/identities/config/useMemberIdentities';
import memberOrder from '@/shared/modules/identities/config/identitiesOrder/member';
import type { Member } from '@/modules/member/types/Member';
import AppIdentitiesHorizontalList from '@/shared/modules/identities/components/identities-horizontal-list.vue';

const props = defineProps<{
  member: Member;
  limit?: number;
  asSvg?: boolean;
}>();

const identities = computed(() => useMemberIdentities({
  member: props.member,
  order: memberOrder.list,
}));
</script>

<script lang="ts">
export default {
  name: 'AppIdentitiesHorizontalListMembers',
};
</script>
