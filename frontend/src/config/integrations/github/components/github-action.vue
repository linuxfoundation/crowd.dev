<template>
  <lf-button v-if="isMapping" type="secondary" @click="isSettingsDrawerOpen = true">
    Map repositories
  </lf-button>
  <app-github-settings-drawer
    v-if="isSettingsDrawerOpen"
    v-model="isSettingsDrawerOpen"
    :integration="props.integration"
  />
</template>

<script setup lang="ts">
import type { GithubMappingIntegration } from '@/config/integrations/github/types/GithubMappingSettings';
import { computed, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import AppGithubSettingsDrawer from '@/config/integrations/github/components/settings/github-settings-drawer.vue';

const props = withDefaults(defineProps<{
  integration: GithubMappingIntegration;
  preventAutoOpen?: boolean;
}>(), {
  preventAutoOpen: false,
});

const isMapping = computed(() => props.integration.status === 'mapping');

const isSettingsDrawerOpen = ref(props.integration.status === 'mapping' && !props.preventAutoOpen);
</script>

<script lang="ts">
export default {
  name: 'LfGithubAction',
};
</script>
