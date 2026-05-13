<template>
  <div class="flex items-center gap-4">
    <!--      <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
    <!--        <lf-icon name="circle-info" type="regular" />-->
    <!--        Details-->
    <!--      </lf-button>-->
    <lf-button type="outline" @click="isDevtoConnectDrawerVisible = true">
      <lf-icon name="link-simple" />
      <slot>Connect</slot>
    </lf-button>
  </div>
  <lf-devto-connect-drawer
    v-if="isDevtoConnectDrawerVisible"
    v-model="isDevtoConnectDrawerVisible"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import { defineProps, ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDevtoConnectDrawer from '@/config/integrations/devto/components/devto-connect-drawer.vue';

const props = defineProps<{
  integration: any,
  segmentId: string | null;
  grandparentId: string | null;
}>();

const isDevtoConnectDrawerVisible = ref(false);
</script>

<script lang="ts">
export default {
  name: 'AppDevtoConnect',
};
</script>
