<template>
  <article class="border-b border-gray-100 py-5">
    <div class="flex justify-between items-center">
      <div class="flex items-center">
        <lf-avatar
          :src="avatar(props.member)"
          :name="props.member.displayName"
          :size="32"
        />
        <div class="pl-3 pr-4">
          <p class="text-medium font-semibold mb-0.5 truncate" style="max-width: 30ch">
            {{ props.member.displayName }}
          </p>
          <p class="text-tiny text-gray-500">
            {{ formatNumber(props.member.activityCount) }} {{ pluralize('activity', +props.member.activityCount) }}
          </p>
        </div>
        <lf-badge size="small" :type="config.badgeType" class="!font-semibold">
          {{ config.badgeText(props.member) }}
        </lf-badge>
      </div>
      <slot name="action" />
    </div>
    <p v-if="description?.length" class="text-small mt-2 text-gray-500" v-html="$sanitize(description)" />
  </article>
</template>

<script lang="ts" setup>
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import { computed } from 'vue';
import type { DataIssueTypeConfig } from '@/modules/data-quality/config/data-issue-types';
import { dataIssueTypes } from '@/modules/data-quality/config/data-issue-types';
import pluralize from 'pluralize';
import { formatNumber } from '@/utils/number';

const props = defineProps<{
  member: Contributor,
  type: SelectedIssueType,
}>();

const { avatar } = useContributorHelpers();

const config = computed<DataIssueTypeConfig>(() => dataIssueTypes[props.type]);

const description = computed<(member: Contributor) => string>(() => config.value.description(props.member));
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityMemberIssuesItem',
};
</script>
