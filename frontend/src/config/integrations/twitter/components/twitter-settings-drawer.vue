<template>
  <app-drawer
    v-model="isVisible"
    custom-class="integration-twitter-drawer"
    title="X/Twitter"
    size="600px"
    pre-title="Integration"
    :pre-title-img-src="logoUrl"
    pre-title-img-alt="X/Twitter logo"
    close-on-click-modal
    :close-function="canClose"
    @close="isVisible = false"
  >
    <template #belowTitle>
      <drawer-description integration-key="twitter" />
    </template>
    <template #content>
      <el-form label-position="top" class="form integration-twitter-form" @submit.prevent>
        <el-form-item label="Track hashtag">
          <el-input v-model="model.hashtag" clearable class="hashtag-input">
            <template #prefix>
              <span>#</span>
            </template>
          </el-input>

          <div class="app-form-hint leading-tight mt-2">
            Tip: Choose a hashtag that's specific to your
            company/community for better data
          </div>
        </el-form-item>
      </el-form>
    </template>

    <template #footer>
      <drawer-footer-buttons
        :integration="integration ?? undefined"
        :is-edit-mode="Number(integration?.settings?.hashtags) > 0"
        :has-form-changed="hasFormChanged"
        :is-loading="false"
        :is-submit-disabled="!hasFormChanged"
        :cancel="() => (isVisible = false)"
        :revert-changes="doReset"
        :connect="connect"
      />
    </template>
  </app-drawer>
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script setup lang="ts">
import type { Integration } from '@/modules/admin/modules/integration/types/Integration';
import type { TwitterSettings } from '@/config/integrations/twitter/types/TwitterSettings';
import { computed, ref } from 'vue';
import isEqual from 'lodash/isEqual';
import twitter from '@/config/integrations/twitter/config';
import config from '@/config';
import { AuthService } from '@/modules/auth/services/auth.service';
import AppDrawer from '@/shared/drawer/drawer.vue';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';

defineOptions({
  name: 'LfTwitterSettingsDrawer',
});

const props = withDefaults(defineProps<{
  modelValue?: boolean;
  integration?: Integration<TwitterSettings> | null;
  segmentId?: string | null;
  grandparentId?: string | null;
}>(), {
  modelValue: false,
  integration: null,
  segmentId: null,
  grandparentId: null,
});

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void}>();
const changesConfirmationModalRef = ref<InstanceType<typeof ChangesConfirmationModal> | null>(null);

const getHashtags = (): string[] => props.integration?.settings?.hashtags || [];

const getConnectUrl = (): string => {
  const redirectUrl = props.grandparentId && props.segmentId
    ? `${window.location.protocol}//${window.location.host}/integrations/${props.grandparentId}/${props.segmentId}?success=true`
    : `${window.location.protocol}//${window.location.host}${window.location.pathname}?success=true`;

  return `${config.backendUrl}/twitter/connect?redirectUrl=${redirectUrl}&crowdToken=${AuthService.getToken()}&segments[]=${
    props.segmentId
  }`;
};

const parsedHashtags = computed<string>(() => (getHashtags().length
  ? getHashtags()[getHashtags().length - 1]
  : ''));
const getInitialModel = (): { hashtag?: string } => (parsedHashtags.value ? { hashtag: parsedHashtags.value } : {});
const model = ref(getInitialModel());

const logoUrl = twitter.image;

const hasFormChanged = computed(
  () => !isEqual(getInitialModel(), model.value),
);

const isVisible = computed({
  get() {
    return props.modelValue;
  },
  set(value: boolean) {
    return emit('update:modelValue', value);
  },
});

const computedConnectUrl = computed(() => {
  const encodedHashtags = model.value.hashtag
    ? `&hashtags[]=${model.value.hashtag}`
    : '';

  return `${getConnectUrl()}${encodedHashtags}`;
});

const connect = () => {
  if (hasFormChanged.value) {
    window.open(computedConnectUrl.value, '_self');
  }
};

const doReset = () => {
  model.value = getInitialModel();
};

const canClose = (done: (cancel?: boolean) => void) => {
  if (hasFormChanged.value) {
    changesConfirmationModalRef.value?.open().then((discardChanges) => {
      if (discardChanges) {
        doReset();
        done(false);
      } else {
        done(true);
      }
    });
  } else {
    done(false);
  }
};
</script>

<style lang="scss">
.integration-twitter-form {
    .el-form-item {
        @apply mb-3;

        &__content {
            @apply mb-0;

            .hashtag-input .el-input__inner {
                @apply pl-1;
            }
        }
    }

    .el-input-group__prepend {
        @apply px-3;
    }
}
</style>
