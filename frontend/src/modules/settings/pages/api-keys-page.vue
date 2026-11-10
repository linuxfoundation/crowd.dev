<template>
  <div class="panel mt-6 grid grid-cols-3 gap-12">
    <div class="col-span-1">
      <h6>API access</h6>
      <div class="text-2xs text-gray-500 mt-1">
        Read our
        <a
          href="https://app.swaggerhub.com/apis-docs/Crowd.dev/Crowd.dev"
          target="_blank"
          rel="noopener noreferrer"
          class="font-semibold hover:underline"
        >documentation</a>
        to get the most out of our API
      </div>
    </div>
    <el-form
      class="col-span-2 api-keys-form flex flex-col gap-2"
      label-position="top"
    >
      <el-form-item label="Auth Token">
        <el-input
          ref="inputRef"
          :value="jwtToken"
          readonly
          :type="showToken ? 'text' : 'password'"
        >
          <template #append>
            <el-tooltip
              v-if="!showToken"
              content="Show Auth Token"
              placement="top"
            >
              <lf-button
                type="secondary-ghost"
                size="tiny"
                class="-mx-5"
                @click="onShowToken"
              >
                <lf-icon name="eye" :size="16" class="!text-large" />
              </lf-button>
            </el-tooltip>
            <el-tooltip
              v-else
              content="Copy to clipboard"
              placement="top"
            >
              <lf-button
                type="secondary-ghost"
                size="tiny"
                class="-mx-5"
                icon-only
                @click="copyToClipboard()"
              >
                <lf-icon name="copy" :size="16" class="!text-large" />
              </lf-button>
            </el-tooltip>
          </template>
        </el-input>
      </el-form-item>
    </el-form>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue';
import type { InputInstance } from 'element-plus';
import { AuthService } from '@/modules/auth/services/auth.service';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';

import { ToastStore } from '@/shared/message/notification';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

defineOptions({ name: 'AppApiKeysPage' });

const inputRef = ref<InputInstance>();
const showToken = ref<boolean>(false);

const jwtToken = computed<string | null>(() => AuthService.getToken());

const { trackEvent } = useProductTracking();

const copyToClipboard = async (): Promise<void> => {
  const toCopy = jwtToken.value ?? '';

  trackEvent({
    key: FeatureEventKey.COPY_AUTH_TOKEN,
    type: EventType.FEATURE,
  });

  await navigator.clipboard.writeText(toCopy);

  ToastStore.success(
    'Tenant ID successfully copied to your clipboard',
  );
};

const onShowToken = (): void => {
  trackEvent({
    key: FeatureEventKey.SHOW_AUTH_TOKEN,
    type: EventType.FEATURE,
  });

  showToken.value = true;
  inputRef.value.focus();
};
</script>

<style lang="scss">
.api-keys-form.el-form {
  .el-form-item {
    @apply mb-0;
  }

  .el-form-item .el-input__wrapper .el-input__inner {
    @apply text-gray-500;
  }

  .el-form-item .el-form-item__label,
  .el-form--default.el-form--label-top
    .el-form-item
    .el-form-item__label:first-of-type {
    @apply mb-0;
  }
}
</style>
