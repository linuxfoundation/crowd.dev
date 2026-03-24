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
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import { useRoute } from 'vue-router';

const route = useRoute();
const props = defineProps<{
  segmentId: string | null;
  grandparentId: string | null;
}>();

const { doDiscordConnect } = mapActions('integration');

const connectUrl = computed(() => config.discordInstallationUrl);

const connect = () => {
  if (props.grandparentId && props.segmentId) {
    localStorage.setItem('segmentId', props.segmentId);
    localStorage.setItem('segmentGrandparentId', props.grandparentId);
  }
  window.open(connectUrl.value, '_self');
};

const finallizeDiscordConnect = () => {
  const {
    source, code,
  } = route.query;
  const guildId = route.query.guild_id;

  if (code && source === 'discord' && guildId) {
    doDiscordConnect({
      guildId,
    })
      .then(() => {
        trackEvent({
          key: FeatureEventKey.CONNECT_INTEGRATION,
          type: EventType.FEATURE,
          properties: { platform: Platform.DISCORD },
        });
      });
  }
};

onMounted(() => {
  finallizeDiscordConnect();
});
</script>

<script lang="ts">
export default {
  name: 'LfDiscordConnect',
};
</script>
