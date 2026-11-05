<template>
  <div class="flex items-center gap-4">
    <!--      <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
    <!--        <lf-icon name="circle-info" type="regular" />-->
    <!--        Details-->
    <!--      </lf-button>-->
    <lf-button type="outline" @click="isDiscourseConnectDrawerOpen = true">
      <lf-icon name="link-simple" />
      <slot>Connect</slot>
    </lf-button>
  </div>
  <lf-discourse-settings-drawer
    v-if="isDiscourseConnectDrawerOpen"
    v-model="isDiscourseConnectDrawerOpen"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import type { Integration } from '@/modules/admin/modules/integration/types/Integration';
import type { DiscourseSettings } from '@/config/integrations/discourse/types/DiscourseSettings';
import { defineProps, ref } from 'vue';
import LfDiscourseSettingsDrawer from '@/config/integrations/discourse/components/discourse-settings-drawer.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps<{
  integration: Integration<DiscourseSettings> | undefined,
  segmentId: string | null;
  grandparentId: string | null;
}>();

const isDiscourseConnectDrawerOpen = ref(false);
</script>

<script lang="ts">
export default {
  name: 'AppDiscourseConnect',
};
</script>
