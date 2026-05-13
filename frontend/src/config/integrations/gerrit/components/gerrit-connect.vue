<template>
  <div class="flex items-center gap-4">
    <!--      <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
    <!--        <lf-icon name="circle-info" type="regular" />-->
    <!--        Details-->
    <!--      </lf-button>-->
    <lf-button type="outline" @click="isGerritConnectDrawerOpen = true">
      <lf-icon name="link-simple" />
      <slot>Connect</slot>
    </lf-button>
  </div>
  <lf-gerrit-settings-drawer
    v-if="isGerritConnectDrawerOpen"
    v-model="isGerritConnectDrawerOpen"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import { defineProps, ref } from 'vue';
import LfGerritSettingsDrawer from '@/config/integrations/gerrit/components/gerrit-settings-drawer.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps<{
  integration: any,
  segmentId: string | null;
  grandparentId: string | null;
}>();

const isGerritConnectDrawerOpen = ref(false);
</script>

<script lang="ts">
export default {
  name: 'LfGerritConnect',
};
</script>
