<template>
  <div class="flex items-center gap-4">
    <!--      <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
    <!--        <lf-icon name="circle-info" type="regular" />-->
    <!--        Details-->
    <!--      </lf-button>-->
    <lf-button type="outline" @click="isJiraSettingsDrawerVisible = true">
      <lf-icon name="link-simple" />
      <slot>Connect</slot>
    </lf-button>
  </div>
  <lf-jira-settings-drawer
    v-if="isJiraSettingsDrawerVisible"
    v-model="isJiraSettingsDrawerVisible"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import { defineProps, ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfJiraSettingsDrawer from '@/config/integrations/jira/components/jira-settings-drawer.vue';

const props = defineProps<{
  integration: any;
  segmentId: string | null;
  grandparentId: string | null;
}>();

const isJiraSettingsDrawerVisible = ref(false);
</script>

<script lang="ts">
export default {
  name: 'LfJiraConnect',
};
</script>
