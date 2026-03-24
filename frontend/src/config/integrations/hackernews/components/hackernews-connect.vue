<template>
  <div class="flex items-center gap-4">
    <!--      <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
    <!--        <lf-icon name="circle-info" type="regular" />-->
    <!--        Details-->
    <!--      </lf-button>-->
    <lf-button type="outline" @click="isHackernewsSettingsDrawerOpen = true">
      <lf-icon name="link-simple" />
      <slot>Connect</slot>
    </lf-button>
  </div>
  <lf-hackernews-settings-drawer
    v-if="isHackernewsSettingsDrawerOpen"
    v-model="isHackernewsSettingsDrawerOpen"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import { defineProps, ref } from 'vue';
import LfHackernewsSettingsDrawer from '@/config/integrations/hackernews/components/hackernews-settings-drawer.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps<{
  integration: any,
  segmentId: string | null;
  grandparentId: string | null;
}>();

const isHackernewsSettingsDrawerOpen = ref(false);

</script>

<script lang="ts">
export default {
  name: 'AppHackerNewsConnect',
};
</script>
