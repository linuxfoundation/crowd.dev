<template>
  <div class="member-engagement-level-wrapper">
    <div class="leading-none">
      <div
        v-if="computedEngagementLevel.value === -1"
        class="inline-flex items-center justify-center"
      >
        <span
          class="block mr-2 text-xs font-semibold text-gray-400"
        >Computing</span>
      </div>
      <div
        v-else-if="member.attributes.isTeamMember?.crowd"
      >
        <span
          class="block mr-2 text-xs font-semibold text-gray-500"
        >-</span>
      </div>
      <el-tooltip
        v-else
        placement="top"
      >
        <template #content>
          Calculated based on the recency and importance of the activities<br>
          a person has performed in relation to the community.
        </template>

        <div

          class="member-engagement-level"
          :class="`member-engagement-level--${computedEngagementLevel.label.toLowerCase()}`"
        >
          <span class="member-engagement-level-value">
            {{ Math.round(computedEngagementLevel.value) }}
          </span>

          <span class="member-engagement-level-label" data-qa="member-engagement-level-label">
            {{ computedEngagementLevel.label }}
          </span>
        </div>
      </el-tooltip>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { Member, MemberAttribute } from '@/modules/member/types/Member';

defineOptions({ name: 'AppCommunityEngagementLevel' });

const props = defineProps<{
  member: Pick<Member, 'score'> & {
    attributes: Record<string, MemberAttribute & { crowd?: boolean }>;
  };
}>();

const computedEngagementLevel = computed(() => {
  const value = props.member.score;
  let label = '';

  if (value <= 1) {
    label = 'Silent';
  } else if (value <= 3) {
    label = 'Quiet';
  } else if (value <= 6) {
    label = 'Engaged';
  } else if (value <= 8) {
    label = 'Fan';
  } else if (value <= 10) {
    label = 'Ultra';
  }

  return {
    value,
    label,
  };
});
</script>

<style lang="scss">
.member-engagement-level-wrapper {
  @apply relative;

  .app-page-spinner {
    min-height: unset;
  }
}

.member-engagement-level {
  @apply inline-flex items-center text-xs leading-none pr-1.5 gap-1 font-medium;

  &-value {
    @apply rounded-md h-4 w-4 flex items-center justify-center text-white text-2xs;
  }

  &--silent {
    @apply text-red-600;
    .member-engagement-level-value {
      @apply bg-red-500;
    }
  }

  &--quiet {
    @apply text-yellow-600;
    .member-engagement-level-value {
      @apply bg-yellow-500;
    }
  }

  &--engaged {
    @apply text-green-600;
    .member-engagement-level-value {
      @apply bg-green-500;
    }
  }

  &--fan {
    @apply text-primary-600;
    .member-engagement-level-value {
      @apply bg-primary-500;
    }
  }

  &--ultra {
    @apply text-purple-600;
    .member-engagement-level-value {
      @apply bg-purple-500;
    }
  }
}
</style>
