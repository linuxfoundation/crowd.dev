<template>
  <app-drawer
    v-model="isVisible"
    custom-class="integration-reddit-drawer"
    title="Reddit"
    size="600px"
    pre-title="Integration"
    has-border
    close-on-click-modal="true"
    :close-function="canClose"
    @close="isVisible = false"
  >
    <template #beforeTitle>
      <img
        class="min-w-6 h-6 mr-2"
        :src="logoUrl"
        alt="Git logo"
      />
    </template>
    <template #belowTitle>
      <drawer-description integration-key="reddit" />
    </template>
    <template #content>
      <div
        class="flex flex-col gap-2 items-start mb-2"
      />
      <el-form
        label-position="top"
        class="form integration-reddit-form"
        @submit.prevent
      >
        <div class="flex flex-col gap-2 items-start">
          <span class="block text-sm font-semibold mb-2">Track subreddit</span>
          <span
            class="text-2xs font-light mb-2 text-gray-600"
          >
            Monitor posts, and their comments, from your
            community's subreddit. <br />
          </span>
          <el-form-item
            v-for="(subreddit, index) of model"
            :key="index"
          >
            <div class="flex w-full gap-2">
              <el-input
                v-model="subreddit.value"
                @blur="handleSubredditValidation(index)"
              >
                <template #prepend>
                  reddit.com/r/
                </template>
                <template #suffix>
                  <div
                    v-if="subreddit.validating"
                    v-loading="subreddit.validating"
                    class="flex items-center justify-center w-6 h-6"
                  />
                </template>
              </el-input>
              <lf-button
                v-if="model.length > 1"
                type="primary-link"
                size="medium"
                icon-only
                class="w-10 h-10"
                @click="deleteItem(index)"
              >
                <lf-icon name="trash-can" :size="20" class="text-black" />
              </lf-button>
            </div>
            <span
              v-if="subreddit.touched && !subreddit.valid"
              class="el-form-item__error pt-1"
            >Subreddit does not exist</span>
          </el-form-item>
        </div>
      </el-form>
    </template>

    <template #footer>
      <drawer-footer-buttons
        :integration="integration"
        :is-edit-mode="integration?.settings?.subreddits.length > 0"
        :has-form-changed="hasFormChanged"
        :is-loading="false"
        :is-submit-disabled="!hasFormChanged || connectDisabled"
        :cancel="() => (isVisible = false)"
        :revert-changes="doReset"
        :connect="hasFormChanged ? connect : () => {}"
      />
    </template>
  </app-drawer>
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script setup lang="ts">
import {
  defineEmits,
  defineProps,
  computed,
  ref,
  watch,
} from 'vue';
import { useThrottleFn } from '@vueuse/core';
import { useStore } from 'vuex';
import Nango from '@nangohq/frontend';
import isEqual from 'lodash/isEqual';
import config from '@/config';
import { IntegrationService } from '@/modules/integration/integration-service';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import reddit from '@/config/integrations/reddit/config';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';

const store = useStore();

const props = defineProps<{
  modelValue: boolean,
  integration: any,
  segmentId: string,
  grandparentId: string,
}>();

const emit = defineEmits(['update:modelValue']);
const subreddits = props.integration?.settings?.subreddits.map((i: any) => ({
  value: i,
  validating: false,
  touched: true,
  valid: true,
})) || [{ value: '', loading: false }];

const { trackEvent } = useProductTracking();
const changesConfirmationModalRef = ref<InstanceType<typeof ChangesConfirmationModal> | null>(null);

const model = ref(JSON.parse(JSON.stringify(subreddits)));

const logoUrl = reddit.image;

const hasFormChanged = computed(
  () => !isEqual(
    subreddits.map((i: any) => i.value),
    model.value.map((i: any) => i.value),
  ),
);
const connectDisabled = computed(() => (
  model.value.filter((s: any) => (
    s.valid === false
        || s.value === ''
        || s.touched !== true
  )).length > 0
));

const isVisible = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    return emit('update:modelValue', value);
  },
});

const deleteItem = (index: number) => {
  model.value.splice(index, 1);
};

const doReset = () => {
  model.value = JSON.parse(JSON.stringify(subreddits));
};

const canClose = (done: (value: boolean) => void) => {
  if (hasFormChanged.value) {
    changesConfirmationModalRef.value?.open().then((discardChanges: boolean) => {
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

const handleSubredditValidation = async (index: number) => {
  try {
    let subreddit = model.value[index].value;

    subreddit = subreddit.replace('https://', '');
    subreddit = subreddit.replace('http://', '');
    subreddit = subreddit.replace('reddit.com', '');
    subreddit = subreddit.replace('/r/', '');
    subreddit = subreddit.replace('r/', '');

    model.value[index].value = subreddit;
    model.value[index].validating = true;

    await IntegrationService.redditValidate(
      model.value[index].value,
    );
    model.value[index].valid = true;
  } catch (e) {
    console.error(e);
    model.value[index].valid = false;
  } finally {
    model.value[index].validating = false;
    model.value[index].touched = true;
  }
};

const callOnboard = useThrottleFn(async () => {
  await store.dispatch('integration/doRedditOnboard', {
    subreddits: model.value.map((i: any) => i.value),
    segmentId: props.segmentId,
    grandparentId: props.grandparentId,
  });

  const isUpdate = !!props.integration?.settings?.subreddits;

  trackEvent({
    key: isUpdate ? FeatureEventKey.EDIT_INTEGRATION_SETTINGS : FeatureEventKey.CONNECT_INTEGRATION,
    type: EventType.FEATURE,
    properties: { platform: Platform.REDDIT },
  });
}, 2000);

const connect = async () => {
  const nango = new Nango({ host: config.nangoUrl });
  try {
    await nango.auth('reddit', `${props.segmentId}-reddit`);
    await callOnboard();
    emit('update:modelValue', false);
  } catch (e) {
    console.error(e);
  }
};

watch(isVisible, (newValue, oldValue) => {
  if (newValue) {
    (window as any).analytics.track('Reddit: connect drawer', {
      action: 'open',
    });
  } else if (newValue === false && oldValue) {
    (window as any).analytics.track('Reddit: connect drawer', {
      action: 'close',
    });
  }
});
</script>

<script lang="ts">
export default {
  name: 'LfRedditSettingsDrawer',
};
</script>

<style lang="scss">
.integration-reddit-form {
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
