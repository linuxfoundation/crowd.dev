<template>
  <div
    class="rounded-md border border-gray-200 bg-white mt-2.5 overflow-hidden shadow-sm"
  >
    <div
      v-if="sourceId || activity.url"
      class="flex items-center justify-between bg-gray-50 h-8 px-4 border-b border-gray-200"
    >
      <div v-if="sourceId" class="text-xs font-medium flex items-center">
        <lf-icon name="code-commit" :size="16" class="mr-1.5" />
        <span>SHA: {{ sourceId }}</span>
      </div>
      <div v-if="activity.url">
        <lf-git-activity-link :activity="activity" />
      </div>
    </div>
    <div v-if="showActivityContent" class="pl-4 pt-3 pb-4 pr-10">
      <app-activity-content
        v-if="activity.body"
        class="text-sm text-gray-900"
        :activity="activity"
        show-more
        :display-thread="false"
      />
    </div>

    <div
      class="flex items-center justify-between h-10 mx-4 border-t border-gray-100"
    >
      <lf-git-attributes :activity="activity" />

      <div v-if="activity.parent" class="flex items-center gap-2">
        <span class="font-medium text-gray-400 text-2xs">{{
          toSentenceCase(activity.parent.display.author ?? '')
        }}</span>
        <div class="flex items-center gap-1.5">
          <app-avatar :entity="activity.member" size="xxs" />

          <app-member-display-name
            class="flex items-center"
            custom-class="text-2xs text-gray-600 font-medium block"
            :member="activity.member"
            with-link
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Activity } from '@/shared/modules/activity/types/Activity';
import AppActivityContent from '@/modules/activity/components/activity-content.vue';
import { computed } from 'vue';
import { toSentenceCase } from '@/utils/string';
import AppMemberDisplayName from '@/modules/member/components/member-display-name.vue';
import AppAvatar from '@/shared/avatar/avatar.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfGitActivityLink from './git-activity-link.vue';
import LfGitAttributes from './git-attributes.vue';

const props = defineProps<{
  activity: Activity;
}>();

const sourceId = computed(() => {
  if (props.activity.type === 'authored-commit') {
    return props.activity.sourceId;
  }

  return props.activity.parent?.sourceId;
});

const showActivityContent = computed(() => {
  if (props.activity.type === 'authored-commit' || props.activity.type === 'committed-commit') {
    return !!props.activity.title || !!props.activity.body;
  }

  return false;
});
</script>
