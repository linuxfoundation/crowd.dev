<template>
  <div>
    <div class="flex items-center gap-4">
      <!--      <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
      <!--        <lf-icon name="circle-info" type="regular" />-->
      <!--        Details-->
      <!--      </lf-button>-->
      <lf-button type="outline" @click="isGitConnectDrawerVisible = true">
        <lf-icon name="link-simple" />
        <slot>Connect</slot>
      </lf-button>
    </div>
    <lf-git-settings-drawer
      v-model="isGitConnectDrawerVisible"
      :integration="props.integration"
      :segment-id="props.segmentId"
      :grandparent-id="props.grandparentId"
    />
  </div>
</template>

<script setup lang="ts">
import { defineProps, ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfGitSettingsDrawer from '@/config/integrations/git/components/git-settings-drawer.vue';

const props = defineProps<{
  integration: any,
  segmentId?: string;
  grandparentId?: string;
}>();

const isGitConnectDrawerVisible = ref(false);
// const isDetailsModalOpen = ref(false);
</script>

<script lang="ts">
export default {
  name: 'AppGitConnect',
};
</script>
