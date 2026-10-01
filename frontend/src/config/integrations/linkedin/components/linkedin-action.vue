<template>
  <lf-button v-if="isPendingAction" type="secondary" @click="isLinkedinSettingsDrawerOpen = true">
    Settings
  </lf-button>
  <lf-linkedin-settings-drawer
    v-if="isLinkedinSettingsDrawerOpen"
    v-model="isLinkedinSettingsDrawerOpen"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import {
  computed,
  defineProps,
  ref,
} from 'vue';
import LfLinkedinSettingsDrawer from '@/config/integrations/linkedin/components/linkedin-settings-drawer.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps<{
  integration: any;
  segmentId: string;
  grandparentId: string;
}>();

const isPendingAction = computed(() => props.integration.status === 'pending-action');

// eslint-disable-next-line vue/no-ref-object-reactivity-loss -- intentional initial snapshot
const isLinkedinSettingsDrawerOpen = ref(isPendingAction.value);
</script>

<script lang="ts">
export default {
  name: 'LfLinkedinAction',
};
</script>
