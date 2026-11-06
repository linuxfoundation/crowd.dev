<template>
  <div class="flex flex-wrap">
    <component
      :is="customComponent"
      :class="`member-display-name ${customClass}`"
      :to="
        withLink
          ? {
            name: 'memberView',
            params: { id: member.id },
            query: { projectGroup: selectedProjectGroup?.id },
          }
          : null
      "
    >
      <span v-html="$sanitize(member.displayName)" />
    </component>
    <app-member-badge v-if="showBadge" :member="member" />
  </div>
</template>

<script setup lang="ts">
import AppMemberBadge from '@/modules/member/components/member-badge.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import type { Member } from '@/modules/member/types/Member';

defineOptions({ name: 'AppMemberDisplayName' });

const props = withDefaults(defineProps<{
  member: Member;
  showBadge?: boolean;
  withLink?: boolean;
  customClass?: string | null;
}>(), {
  showBadge: true,
  withLink: false,
  customClass: null,
});

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const customComponent = props.withLink ? 'router-link' : 'span';
</script>
