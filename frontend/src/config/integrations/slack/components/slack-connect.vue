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
import { computed, onMounted } from 'vue';
import config from '@/config';

import { useRoute, useRouter } from 'vue-router';
import { AuthService } from '@/modules/auth/services/auth.service';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';

const { trackEvent } = useProductTracking();
const route = useRoute();
const router = useRouter();
const props = defineProps<{
  segmentId: string | null;
  grandparentId: string | null;
}>();

const connectUrl = computed<string>(() => {
  const redirectUrl = props.grandparentId && props.segmentId
    ? `${window.location.protocol}//${window.location.host}/integrations/${props.grandparentId}/${props.segmentId}?slack-success=true`
    : `${window.location.protocol}//${window.location.host}${window.location.pathname}?slack-success=true`;

  trackEvent({
    key: FeatureEventKey.CONNECT_INTEGRATION,
    type: EventType.FEATURE,
    properties: { platform: Platform.SLACK },
  });

  return `${config.backendUrl}/slack/connect?redirectUrl=${redirectUrl}&crowdToken=${AuthService.getToken()}&segments[]=${props.segmentId}`;
});

const connect = () => {
  window.open(connectUrl.value, '_self');
};

const slackLogo = new URL(
  '@/assets/images/integrations/slack.png',
  import.meta.url,
).href;

const finallizeSlackConnect = () => {
  const slackSuccess = route.query['slack-success'];
  if (slackSuccess) {
    ConfirmDialog({
      vertical: true,
      type: 'custom',
      icon: `<img src="${slackLogo}" class="h-8 min-w-8" alt="slack logo" />`,
      title: `<span class="flex items-start gap-1">Connect Slack bot
              <span class="text-primary-500 text-3xs leading-3 pt-1 font-normal">Required</span></span>`,
      titleClass: 'text-lg',
      message: `
            To fetch data from Slack, you need to install the LFX Slack bot and add it to all channels you want to track. <br><br>
            You can either add the Slack bot directly from a channel, or add the app via channel Integrations.`,
      confirmButtonText: 'How to connect Slack bot',
      showCancelButton: false,
      messageClass: 'text-xs !leading-5 !mt-1 text-gray-600',
      verticalCustomClass: 'custom-slack-message-box',
      closeOnClickModal: false,
      hideCloseButton: true,
    }).then(() => {
      window.open(
        'https://docs.linuxfoundation.org/lfx/community-management/integrations/slack#add-slack-bots-to-channels',
        '_blank',
      );
      router.replace({ query: null });
    });
  }
};

onMounted(() => {
  finallizeSlackConnect();
});
</script>

<script lang="ts">
export default {
  name: 'LfSlackConnect',
};
</script>
