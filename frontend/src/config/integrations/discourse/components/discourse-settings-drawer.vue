<template>
  <app-drawer
    v-model="isVisible"
    custom-class="integration-reddit-drawer"
    title="Discourse"
    size="600px"
    pre-title="Integration"
    :show-footer="true"
    has-border
    close-on-click-modal="true"
    :close-function="canClose"
    @close="handleCancel()"
  >
    <template #beforeTitle>
      <img
        class="min-w-6 h-6 mr-2"
        :src="logoUrl"
        alt="Discourse logo"
      />
    </template>
    <template #belowTitle>
      <drawer-description integration-key="discourse" />
    </template>
    <template #content>
      <el-form
        label-position="top"
        class="form"
        @submit.prevent
      >
        <app-form-item
          class="mb-6"
          :validation="$v.discourseURL"
          label="Discourse URL"
          :required="true"
          :error-messages="{
            required: 'This field is required',
            url: 'Enter valid URL',
          }"
        >
          <div
            class="text-2xs text-gray-500 leading-normal mb-1"
          >
            Hostname of your community instance in Discourse.
          </div>
          <el-input
            ref="focus"
            v-model="form.discourseURL"
            placeholder="https://community.lfx.dev"
            @blur="onBlurDiscourseURL()"
          />
        </app-form-item>
        <app-form-item
          class="mb-6"
          :validation="$v.apiKey"
          label="API Key"
          :required="true"
          :error-messages="{
            required: 'This field is required',
          }"
        >
          <div
            class="text-2xs text-gray-500 leading-normal mb-1"
          >
            Create a new API key in your Discourse account's settings page. You must be an admin user to connect your acount. <a href="https://docs.crowd.dev/docs/discourse-integration#api-key" target="_blank" rel="noopener noreferrer" class="hover:underline">Read more</a>
          </div>
          <el-input
            ref="focus"
            v-model="form.apiKey"
            type="password"
            @blur="onBlurAPIKey()"
          >
            <template #suffix>
              <div
                v-if="isValidating"
                v-loading="isValidating"
                class="flex items-center justify-center w-6 h-6"
              />
            </template>
          </el-input>
        </app-form-item>
      </el-form>
      <el-divider />
      <div class="w-full flex flex-col mb-6" :class="isAPIConnectionValid ? 'opacity-100' : 'opacity-50'">
        <p class="text-[16px] font-semibold">
          Connect webhooks
        </p>
        <div
          class="text-2xs text-gray-500 leading-normal mb-1"
        >
          Create new webhooks in your Discourse account's settings page with the following credentials. <a href="https://docs.crowd.dev/docs/discourse-integration#webhooks" target="_blank" rel="noopener noreferrer" class="hover:underline">Read more</a>
        </div>
      </div>
      <el-form
        class="flex flex-col"
        label-position="top"
        :class="isAPIConnectionValid ? 'opacity-100' : 'opacity-50'"
      >
        <app-form-item label="Payload URL">
          <el-input :value="payloadURL" :readonly="true" :disabled="!isAPIConnectionValid">
            <template #append>
              <el-tooltip
                content="Copy to clipboard"
                placement="top"
              >
                <lf-button
                  type="secondary-ghost"
                  size="tiny"
                  class="-mx-5"
                  icon-only
                  :disabled="!isAPIConnectionValid"
                  @click="copyToClipboard('url')"
                >
                  <lf-icon name="copy" />
                </lf-button>
              </el-tooltip>
            </template>
          </el-input>
        </app-form-item>
        <app-form-item label="Webhook Secret">
          <el-input
            ref="inputRef"
            :value="webhookSecret"
            readonly
            :disabled="!isAPIConnectionValid"
            :type="showToken ? 'text' : 'password'"
          >
            <template #append>
              <el-tooltip
                v-if="!showToken"
                content="Show Webhook Secret"
                placement="top"
              >
                <lf-button
                  type="secondary-ghost"
                  size="tiny"
                  class="-mx-5"
                  icon-only
                  :disabled="!isAPIConnectionValid"
                  @click="onShowToken"
                >
                  <lf-icon name="eye" />
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
                  :disabled="!isAPIConnectionValid"
                  @click="copyToClipboard('secret')"
                >
                  <lf-icon name="copy" />
                </lf-button>
              </el-tooltip>
            </template>
          </el-input>
        </app-form-item>
      </el-form>
      <lf-card v-if="isAPIConnectionValid && props.integration?.settings?.forumHostname" shadow="never" class="rounded-[6px] p-5">
        <div class="mb-3 flex flex-row w-full justify-between">
          <lf-button
            type="secondary-gray"
            size="medium"
            :disabled="!!isWebhookVerifying"
            @click="verifyWebhook()"
          >
            Verify webhook
          </lf-button>
          <div v-if="isWebhookVerifying == null" />
          <div v-else-if="isWebhookVerifying">
            <lf-icon name="circle-notch" :size="14" class="animate-spin text-gray-900 mr-2" />
            <span class="text-gray-900 text-sm">Verifying</span>
          </div>
          <div v-else-if="isWebhookValid">
            <lf-icon name="check" :size="14" class="text-green-500 mr-2" />
            <span class="text-green-500 text-sm">Succesfully verified</span>
          </div>
          <div v-else>
            <lf-icon name="circle-exclamation" :size="14" class="text-red-500 mr-2" />
            <span class="text-red-500 text-sm">No webhooks received yet</span>
          </div>
        </div>
        <div
          class="text-2xs text-gray-500 leading-normal mb-1"
        >
          Confirm if your webhooks are properly configured and LFX is receiving data from Discourse.
        </div>
      </lf-card>
    </template>

    <template #footer>
      <drawer-footer-buttons
        :integration="props.integration"
        :is-edit-mode="!!integration?.settings?.forumHostname"
        :has-form-changed="hasFormChanged"
        :is-loading="loading"
        :is-submit-disabled="$v.$invalid || !hasFormChanged || loading"
        :cancel="handleCancel"
        :revert-changes="revertChanges"
        :connect="connect"
      />
    </template>
  </app-drawer>
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script setup lang="ts">
import {
  computed, defineEmits, defineProps, onMounted, reactive, ref,
} from 'vue';
import discourse from '@/config/integrations/discourse/config';
import { helpers, required, url } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import AppDrawer from '@/shared/drawer/drawer.vue';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import AppFormItem from '@/shared/form/form-item.vue';
import formChangeDetector from '@/shared/form/form-change';
// import elementChangeDetector from '@/shared/form/element-change';
import { IntegrationService } from '@/modules/integration/integration-service';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfCard from '@/ui-kit/card/Card.vue';
import { ToastStore } from '@/shared/message/notification';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';

const { trackEvent } = useProductTracking();
const changesConfirmationModalRef = ref<InstanceType<typeof ChangesConfirmationModal> | null>(null);

const props = defineProps<{
  modelValue: boolean,
  integration: any,
  segmentId: string | null;
  grandparentId: string | null;
}>();

const inputRef = ref();
const showToken = ref(false);
const payloadURL = ref<string | undefined>(undefined);
const webhookSecret = ref(undefined);

const { doDiscourseConnect } = mapActions('integration');

const copyToClipboard = async (type) => {
  const toCopy = type === 'url' ? payloadURL : webhookSecret;
  await navigator.clipboard.writeText(toCopy.value);

  ToastStore.success(
    `${
      type === 'url' ? 'Payload URL' : 'Webhook Secret'
    } successfully copied to your clipboard`,
  );
};

const onShowToken = () => {
  showToken.value = true;
  inputRef.value.focus();
};

function generateRandomSecret(length) {
  const array = new Uint8Array(length);
  window.crypto.getRandomValues(array);
  return Array.from(array).map((b) => b.toString(16).padStart(2, '0')).join('');
}

const form = reactive({
  discourseURL: '',
  apiKey: '',
});

const isValidating = ref(false);
const isAPIConnectionValid = ref(false);
const loading = ref(false);
const isWebhookVerifying = ref(null);
const isWebhookValid = ref(false);

// validate that url doesn't end with a slash
const validateURL = (value) => {
  if (value && value.endsWith('/')) {
    return false;
  }
  return true;
};

const rules = {
  discourseURL: {
    required,
    url,
    noSlash: helpers.withMessage('URL cannot end with a slash', validateURL),
  },
  apiKey: {
    required,
  },
};

const $externalResults = ref({});

const $v = useVuelidate(rules, form, { $externalResults, $stopPropagation: true });

// validate method
async function validate() {
  $v.value.$clearExternalResults();
  // check if everything is valid
  if ($v.value.$error) return;

  isValidating.value = true;

  try {
    await IntegrationService.discourseValidateAPI(form.discourseURL, form.apiKey);
    isAPIConnectionValid.value = true;

    if (!payloadURL.value) {
      payloadURL.value = `${window.location.origin}/api/webhooks/discourse`;
    }
    if (!webhookSecret.value) {
      webhookSecret.value = generateRandomSecret(32);
    }
  } catch {
    $externalResults.value = {
      apiKey: 'Invalid credentials',
    };
    isAPIConnectionValid.value = false;
  }

  isValidating.value = false;
}

const onBlurDiscourseURL = async () => {
  $v.value.discourseURL.$touch();
  await validate();
};

const onBlurAPIKey = async () => {
  $v.value.apiKey.$touch();
  await validate();
};

const emit = defineEmits(['update:modelValue']);

const { hasFormChanged, formSnapshot } = formChangeDetector(form);

const logoUrl = discourse.image;

const isVisible = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    return emit('update:modelValue', value);
  },
});

const canClose = (done: (value: boolean) => void) => {
  if (hasFormChanged.value) {
    changesConfirmationModalRef.value?.open().then((discardChanges: boolean) => {
      if (discardChanges) {
        revertChanges();
        done(false);
      } else {
        done(true);
      }
    });
  } else {
    done(false);
  }
};

const handleCancel = () => {
  emit('update:modelValue', false);
  if (!props.integration?.settings?.forumHostname) {
    form.apiKey = '';
    form.discourseURL = '';
    isValidating.value = false;
    isAPIConnectionValid.value = false;
    loading.value = false;
    isWebhookVerifying.value = null;
    isWebhookValid.value = false;
    $externalResults.value = {};
    $v.value.$reset();
  } else {
    form.discourseURL = props.integration?.settings?.forumHostname;
    form.apiKey = props.integration?.settings.apiKey;
    webhookSecret.value = props.integration?.settings.webhookSecret;
    payloadURL.value = `${window.location.origin}/api/webhooks/discourse`;
    isAPIConnectionValid.value = true;
    isWebhookVerifying.value = null;
    isWebhookValid.value = false;
    $externalResults.value = {};
    $v.value.$reset();
  }
  formSnapshot();
};

const syncData = () => {
  if (props.integration?.settings?.forumHostname) {
    form.discourseURL = props.integration?.settings.forumHostname;
    form.apiKey = props.integration?.settings.apiKey;
    webhookSecret.value = props.integration?.settings.webhookSecret;
    payloadURL.value = `${window.location.origin}/api/webhooks/discourse`;
    isAPIConnectionValid.value = true;
  }
  formSnapshot();
};

const revertChanges = () => {
  syncData();
};

onMounted(() => {
  syncData();
});

const verifyWebhook = async () => {
  isWebhookVerifying.value = true;
  try {
    const webhookVerified = await IntegrationService.discourseVerifyWebhook(props.integration?.id);
    isWebhookValid.value = !!webhookVerified;
  } catch {
    isWebhookValid.value = false;
  } finally {
    isWebhookVerifying.value = false;
  }
};

const connect = async () => {
  loading.value = true;

  const isUpdate = props.integration?.settings?.forumHostname;

  doDiscourseConnect({
    forumHostname: form.discourseURL,
    apiKey: form.apiKey,
    webhookSecret: webhookSecret.value,
    isUpdate,
    segmentId: props.segmentId,
    grandparentId: props.grandparentId,
  })
    .then(() => {
      trackEvent({
        key: isUpdate ? FeatureEventKey.EDIT_INTEGRATION_SETTINGS : FeatureEventKey.CONNECT_INTEGRATION,
        type: EventType.FEATURE,
        properties: {
          platform: Platform.DISCOURSE,
        },
      });

      isVisible.value = false;
    })
    .finally(() => {
      loading.value = false;
    });

  formSnapshot();
};
</script>

<script lang="ts">
export default {
  name: 'LfDiscourseSettingsDrawer',
};
</script>
