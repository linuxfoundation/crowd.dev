<template>
  <div class="flex items-center gap-4">
    <!--    <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
    <!--      <lf-icon name="circle-info" type="regular" />-->
    <!--      Details-->
    <!--    </lf-button>-->
    <lf-button type="outline" @click="isSettingsDrawerOpen = true">
      <lf-icon name="link-simple" />
      <slot>Connect</slot>
    </lf-button>
  </div>
  <lf-groupsio-settings-drawer
    v-if="isSettingsDrawerOpen"
    v-model="isSettingsDrawerOpen"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import { ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfGroupsioSettingsDrawer from '@/config/integrations/groupsio/components/groupsio-settings-drawer.vue';

const props = defineProps<{
  integration: any,
  segmentId?: string,
  grandparentId?: string,
}>();

const isSettingsDrawerOpen = ref(false);
</script>

<script lang="ts">
export default {
  name: 'LfGroupsioConnect',
};
</script>
