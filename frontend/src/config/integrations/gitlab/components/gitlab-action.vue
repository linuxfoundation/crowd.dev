<template>
  <lf-button v-if="isMapping" type="secondary" @click="isSettingsDrawerOpen = true">
    Map repositories
  </lf-button>
  <app-gitlab-settings-drawer
    v-if="isSettingsDrawerOpen"
    v-model="isSettingsDrawerOpen"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import type { GitlabIntegration } from '@/config/integrations/gitlab/types/GitlabSettings';
import { computed, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import AppGitlabSettingsDrawer from './gitlab-settings-drawer.vue';

const props = defineProps<{
  integration: GitlabIntegration;
  segmentId: string | null;
  grandparentId: string | null;
}>();

const isMapping = computed(() => props.integration.status === 'mapping');

// eslint-disable-next-line vue/no-ref-object-reactivity-loss -- intentional initial snapshot
const isSettingsDrawerOpen = ref(isMapping.value);
</script>

<script lang="ts">
export default {
  name: 'AppGitlabAction',
};
</script>
