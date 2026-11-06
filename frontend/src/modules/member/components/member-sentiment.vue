<template>
  <div class="member-sentiment">
    <el-tooltip
      v-if="
        member.averageSentiment
          && member.averageSentiment > 0
      "
      effect="dark"
      :content="`Avg sentiment score: ${member.averageSentiment}`"
      placement="top"
    >
      <lf-icon
        v-if="label === 'Positive'"
        name="face-smile"
        class="text-green-600"
      />
      <lf-icon
        v-else-if="label === 'Neutral'"
        name="face-meh"
        class="text-gray-400"
      />
      <lf-icon
        v-else
        name="face-frown"
        class="text-red-500"
      />
    </el-tooltip>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { Member } from '@/modules/member/types/Member';

defineOptions({ name: 'AppMemberSentiment' });

const props = defineProps<{
  member: Member;
}>();

const label = computed(() => {
  const sentiment = props.member.averageSentiment ?? 0;
  if (sentiment >= 67) {
    return 'Positive';
  } if (sentiment <= 33) {
    return 'Negative';
  }
  return 'Neutral';
});
</script>

<style lang="scss">
.member-sentiment,
.member-sentiment i {
  @apply inline-flex items-center;
}
</style>
