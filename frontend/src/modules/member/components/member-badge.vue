<template>
  <div
    v-if="isTeam || isBot"
    class="member-badge flex items-center ml-1 min-w-fit"
  >
    <el-tooltip
      v-if="isTeam"
      placement="top"
      :content="computedTooltipContent('team')"
    >
      <div class="inline-flex text-2xs px-1 font-medium text-white rounded h-4 items-center justify-center bg-gray-400">
        Team
      </div>
    </el-tooltip>
    <el-tooltip
      v-if="isBot"
      placement="top"
      :content="computedTooltipContent('bot')"
    >
      <div class="inline-flex text-2xs px-1 font-medium text-white rounded h-4 items-center justify-center bg-gray-400">
        Bot
      </div>
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { Member } from '@/modules/member/types/Member';

defineOptions({ name: 'AppMemberBadge' });

const props = defineProps<{
  member: Member;
}>();

const isTeam = computed(() => props.member.attributes.isTeamMember?.default);

const isBot = computed(() => props.member.attributes.isBot?.default);

const computedTooltipContent = (tooltip: 'team' | 'bot'): string => {
  if (tooltip === 'team') {
    return 'This person belongs to your company';
  }
  return 'Bot';
};
</script>

<style lang="scss">
.flex > .member-display-name + .member-badge,
.inline-flex > .member-display-name + .member-badge {
  @apply ml-2;
}
</style>
