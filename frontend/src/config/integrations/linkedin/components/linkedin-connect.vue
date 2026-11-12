<template>
  <div class="flex items-center gap-4">
    <!--      <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
    <!--        <lf-icon name="circle-info" type="regular" />-->
    <!--        Details-->
    <!--      </lf-button>-->
    <lf-button type="outline" @click="connect()">
      <lf-icon name="link-simple" />
      <slot>Connect</slot>
    </lf-button>
  </div>
</template>

<script setup lang="ts">
import Nango from '@nangohq/frontend';
import { useThrottleFn } from '@vueuse/core';
import config from '@/config';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { useIntegrationStore } from '@/modules/integration/store/integration.store';

defineOptions({
  name: 'AppLinkedInConnect',
});

const props = defineProps<{
  segmentId: string;
  grandparentId: string;
}>();

const { connectLinkedin } = useIntegrationStore();

const callOnboard = useThrottleFn(async () => {
  await connectLinkedin({
    segmentId: props.segmentId,
    grandparentId: props.grandparentId,
  });
}, 2000);

const connect = async () => {
  const nango = new Nango({ host: config.nangoUrl });

  try {
    await nango.auth(
      'linkedin',
      `${props.segmentId}-linkedin`,
    );
    await callOnboard();
  } catch (e) {
    console.error(e);
  }
};
</script>
