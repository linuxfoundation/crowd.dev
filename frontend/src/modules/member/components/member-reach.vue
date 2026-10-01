<template>
  <div class="reach">
    <el-tooltip
      placement="top-start"
      :disabled="reach.total === -1 || (!reach.github && !reach.twitter)"
    >
      <template #content>
        <div>
          <div v-if="!!reach.github">
            Github:
            <span class="font-semibold">
              {{
                formatNumberToCompact(reach.github)
              }}
              {{
                pluralize('follower', reach.github, false)
              }}
            </span>
          </div>
          <div v-if="!!reach.twitter">
            X/Twitter:
            <span class="font-semibold">
              {{
                formatNumberToCompact(reach.twitter)
              }}
              {{
                pluralize('follower', reach.twitter, false)
              }}
            </span>
          </div>
        </div>
      </template>
      <div>
        <span v-if="!!reach.total && reach.total !== -1">
          {{
            formatNumberToCompact(reach.total)
          }}
        </span>
        <span v-else>-</span>
      </div>
    </el-tooltip>
  </div>
</template>

<script setup>
import { formatNumberToCompact } from '@/utils/number';
import pluralize from 'pluralize';
import { computed } from 'vue';

const props = defineProps({
  member: {
    type: Object,
    default: () => {},
  },
});

const reach = computed(() => {
  if (typeof props.member.reach === 'number') {
    return {
      total: props.member.reach,
    };
  }

  return props.member.reach;
});
</script>

<style lang="scss">
.reach {
  .app-page-spinner {
    @apply min-h-6;
  }
}
</style>
