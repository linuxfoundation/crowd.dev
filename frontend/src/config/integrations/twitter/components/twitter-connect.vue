<template>
  <div class="flex items-center gap-4">
    <!--    <lf-button type="secondary-ghost" @click="isDetailsModalOpen = true">-->
    <!--      <lf-icon name="circle-info" type="regular" />-->
    <!--      Details-->
    <!--    </lf-button>-->
    <lf-button type="outline" @click="connect()">
      <lf-icon name="link-simple" />
      <slot>Connect</slot>
    </lf-button>
  </div>
</template>

<script setup lang="ts">
import {
  defineProps, computed, onMounted,
} from 'vue';
import { useRouter, useRoute } from 'vue-router';
import config from '@/config';
import { ToastStore } from '@/shared/message/notification';
import { AuthService } from '@/modules/auth/services/auth.service';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const route = useRoute();
const router = useRouter();

const props = defineProps<{
  segmentId: string | null;
  grandparentId: string | null;
  integration: any;
}>();

// Only render twitter drawer and settings button, if integration has settings
const hashtags = computed(() => props.integration?.settings?.hashtags || []);

// Create an url for the connection without the hashtags
// This will allow to be reused by the twitter drawer component
// and override the current configured hashtag
const connectUrl = computed(() => {
  const redirectUrl = props.grandparentId && props.segmentId
    ? `${window.location.protocol}//${window.location.host}/integrations/${props.grandparentId}/${props.segmentId}?success=true`
    : `${window.location.protocol}//${window.location.host}${window.location.pathname}?success=true`;

  return `${config.backendUrl}/twitter/connect?redirectUrl=${redirectUrl}&crowdToken=${AuthService.getToken()}&segments[]=${
    props.segmentId
  }`;
});

const connect = () => {
  // Add the already configured hashtags to the connectUrl
  const encodedHashtags = hashtags.value.length > 0
    ? `&hashtags[]=${hashtags.value[hashtags.value.length - 1]}`
    : '';

  window.open(`${connectUrl.value}${encodedHashtags}`, '_self');
};

const finallizeTwitterConnection = () => {
  const isConnectionSuccessful = route.query.success;
  const twitterError = route.query['twitter-error'];

  if (isConnectionSuccessful) {
    router.replace({ query: null });
    ToastStore.success('Integration updated successfully');
  }

  if (twitterError) {
    ToastStore.error('Something went wrong during X/Twitter OAuth. Please try again later.');
  }
};

onMounted(() => {
  finallizeTwitterConnection();
});
</script>

<script lang="ts">
export default {
  name: 'LfTwitterConnect',
};
</script>
