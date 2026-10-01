<template>
  <a
    :href="url"
    class="text-2xs text-gray-500 font-medium flex items-center"
    target="_blank"
    rel="noopener noreferrer"
    @click.stop
  >
    <lf-icon name="arrow-up-right-from-square" :size="14" class="mr-1" />
    <span class="block">Open on Git Remote</span>
  </a>
</template>

<script setup lang="ts">
import type { Activity } from '@/shared/modules/activity/types/Activity';
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  activity: Activity;
}>();

const url = computed(() => {
  const sourceId = props.activity.sourceParentId
    ? props.activity.sourceParentId
    : props.activity.sourceId;

  const { channel } = props.activity;
  const urlResource = new URL(channel);
  const domain = urlResource.hostname;
  const path = urlResource.pathname;

  // this is a special case, we need to handle it differently like gerrit
  if (domain.startsWith('git.opendaylight')) {
    const repoName = path.split('/').pop();
    // remove .git from the end of the repoName
    const repoNameNoGit = repoName?.replace(/\.git$/, '');
    return `https://${domain}/gerrit/gitweb?p=${repoNameNoGit}.git;a=commit;h=${sourceId}`;
  }

  // we need to handle cases for gerrir, github, gitlab, git
  if (domain.startsWith('git.')) {
    // remove .git from the end of the channel
    const channelNoGit = channel.replace(/\.git$/, '');
    return `${channelNoGit}.git/commit/?id=${sourceId}`;
  }

  if (domain.startsWith('gerrit.')) {
    const repoName = path.split('/').pop();
    // remove .git from the end of the repoName
    const repoNameNoGit = repoName?.replace(/\.git$/, '');
    return `https://${domain}/gerrit/gitweb?p=${repoNameNoGit}.git;a=commit;h=${sourceId}`;
  }

  if (domain === 'github.com') {
    return `${channel}/commit/${sourceId}`;
  }

  // default
  return `${channel}/commit/${sourceId}`;
});
</script>
