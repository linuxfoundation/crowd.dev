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
import {
  defineProps, computed, onMounted, ref,
} from 'vue';
import { useRoute } from 'vue-router';
import config from '@/config';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import { AuthService } from '@/modules/auth/services/auth.service';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { mapActions } from '@/shared/vuex/vuex.helpers';

const route = useRoute();

const props = defineProps<{
  integration: any,
  segmentId: string | null,
  grandparentId: string | null,
}>();

const { doGitlabConnect } = mapActions('integration');

const isFinishingModalOpen = ref(false);

const connectUrl = computed(() => `${config.backendUrl}/gitlab/connect?crowdToken=${AuthService.getToken()}&segments[]=${props.segmentId}`);

const connect = async () => {
  try {
    if (props.grandparentId && props.segmentId) {
      localStorage.setItem('segmentId', props.segmentId);
      localStorage.setItem('segmentGrandparentId', props.grandparentId);
    }
    const result = await ConfirmDialog({
      type: 'notification',
      title: 'Are you the admin of your GitLab organization?',
      titleClass: 'text-lg pt-2',
      message: `Only GitLab users with admin permissions are able to connect LFX's GitLab integration.
        If you are an organization member, you will need an approval from the GitLab workspace admin. <a href="https://docs.crowd.dev/docs/github-integration" target="_blank">Read more</a>`,
      icon: 'fa-circle-info fa-light',
      confirmButtonText: "I'm the GitLab organization admin",
      cancelButtonText: 'Invite organization admin to this workspace',
      verticalCancelButtonClass: 'hidden',
      verticalConfirmButtonClass: 'btn btn--md btn--primary w-full !mb-2',
      vertical: true,
      distinguishCancelAndClose: true,
      autofocus: false,
      messageClass: 'text-xs !leading-5 !mt-1 text-gray-600',
    });

    if (result) {
      window.open(connectUrl.value, '_self');
    }
  } catch (error) {
    console.error('Error connecting to GitLab:', error);
    // Handle error (e.g., show an error message to the user)
  }
};

const finallizeGitlabConnect = () => {
  const {
    code, source, state,
  } = route.query;

  if (code && source === 'gitlab') {
    isFinishingModalOpen.value = true;
    doGitlabConnect({
      code,
      state,
      segmentId: props.segmentId,
      grandparentId: props.grandparentId,
    })
      .then(() => {
        isFinishingModalOpen.value = false;
      });
  }
};

onMounted(() => {
  finallizeGitlabConnect();
});
</script>

<script lang="ts">
export default {
  name: 'LfGitlabConnect',
};
</script>
