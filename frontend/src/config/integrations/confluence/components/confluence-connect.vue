<template>
  <lf-button
    type="outline"
    @click="isConfluenceSettingsDrawerVisible = true"
  >
    <lf-icon name="link-simple" />
    <slot>Connect</slot>
  </lf-button>

  <lf-confluence-settings-drawer
    v-if="isConfluenceSettingsDrawerVisible"
    v-model="isConfluenceSettingsDrawerVisible"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />
</template>

<script setup lang="ts">
import { defineProps, ref } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfConfluenceSettingsDrawer from '@/config/integrations/confluence/components/confluence-settings-drawer.vue';

const props = defineProps<{
  integration: any;
  segmentId: string;
  grandparentId: string;
}>();

const isConfluenceSettingsDrawerVisible = ref(false);
</script>

<script lang="ts">
export default {
  name: 'LfConfluenceConnect',
};
</script>
