<template>
  <app-drawer
    v-model="isVisible"
    custom-class="integration-gerrit-drawer"
    size="600px"
    title="Gerrit"
    pre-title="Integration"
    has-border
    close-on-click-modal="true"
    :close-function="canClose"
    @close="cancel"
  >
    <template #beforeTitle>
      <img class="min-w-6 h-6 mr-2" :src="logoUrl" alt="Gerrit logo" />
    </template>
    <template #belowTitle>
      <drawer-description integration-key="gerrit" />
    </template>
    <template #content>
      <div class="flex gap-2 bg-brand-50 -mt-5 -mx-6 py-3 px-6 mb-5">
        <div>
          <lf-icon name="circle-info" type="solid" class="text-brand-500" :size="20" />
        </div>
        <div class="text-xs text-brand-800">
          Connected repositories are also synced through Git, which automatically mirrors your
          Gerrit settings for adding, updating, and deleting repositories.
        </div>
      </div>
      <lf-gerrit-settings-empty v-if="showEmptyState" @add="showEmptyState = false" />
      <div v-else class="flex flex-col gap-5 items-start">
        <div class="flex flex-col gap-2 w-full">
          <label for="devUrl" class="text-gray-900 text-small">
            Gerrit organization URL<span class="text-red-500">*</span>
          </label>
          <div class="flex justify-between w-full gap-5 items-center">
            <el-form class="w-full" @submit.prevent>
              <el-input
                id="devUrl"
                v-model="form.orgURL"
                class="text-green-500 is-rounded"
                spellcheck="false"
                placeholder="Enter organization URL"
              />
            </el-form>

            <div class="flex-shrink-0">
              <lf-switch
                v-model="form.enableAllRepos"
                :size="'small'"
                :disabled="form.orgURL.length === 0"
                :class="{ 'opacity-50': form.orgURL.length === 0 }"
              >
                <template #default>
                  <span class="text-gray-900 text-small mr-2">All remote URLs</span>
                </template>
              </lf-switch>
            </div>
          </div>
        </div>

        <hr class="w-full border border-solid border-gray-200" />

        <template v-if="!form.enableAllRepos">
          <lf-button
            type="primary-link"
            size="medium"
            :disabled="form.orgURL.length === 0"
            :class="{ 'opacity-50': form.orgURL.length === 0 }"
            @click="addRepoName()"
          >
            + Add remote URL
          </lf-button>
          <el-form class="w-full" @submit.prevent>
            <app-array-input
              v-for="(_, ii) of form.repoNames"
              :key="ii"
              v-model="form.repoNames[ii]"
              class="text-green-500 pb-5"
              placeholder="Enter repository name"
              :input-label="form.orgURL"
            >
              <template #after>
                <lf-button
                  type="secondary-link"
                  size="medium"
                  class="w-10 h-10"
                  @click="removeRepoName(ii)"
                >
                  <lf-icon name="circle-xmark" :size="20" />
                </lf-button>
              </template>
            </app-array-input>
          </el-form>
        </template>
        <div v-else class="text-gray-500 text-xs flex items-start gap-2">
          <lf-icon name="circle-info" :size="16" class="mt-0.5" />
          All repositories under the selected Gerrit organization are automatically synced.
          Manual repository selection is disabled to avoid conflicts.
        </div>
      </div>
    </template>

    <template #footer>
      <drawer-footer-buttons
        :integration="props.integration"
        :is-edit-mode="!!props.integration?.settings"
        :has-form-changed="hasFormChanged"
        :is-loading="loading"
        :is-submit-disabled="$v.$invalid || !hasFormChanged || loading"
        :cancel="cancel"
        :revert-changes="revertChanges"
        :connect="connect"
        :connect-tooltip-content="connectTooltipContent"
      />
    </template>
  </app-drawer>
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script setup lang="ts">
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import {
  computed, onMounted, reactive, ref,
} from 'vue';
import gerrit from '@/config/integrations/gerrit/config';
import formChangeDetector from '@/shared/form/form-change';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import AppArrayInput from '@/shared/form/array-input.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';
import { parseDuplicateRepoError, customRepoErrorMessage } from '@/shared/helpers/error-message.helper';
import { IntegrationService } from '@/modules/integration/integration-service';
import { ToastStore } from '@/shared/message/notification';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import LfGerritSettingsEmpty from '@/config/integrations/gerrit/components/gerrit-settings-empty.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';

const emit = defineEmits(['update:modelValue']);
const props = defineProps<{
  integration: any;
  modelValue: boolean;
  segmentId: string | null;
  grandparentId: string | null;
}>();

const { trackEvent } = useProductTracking();
const changesConfirmationModalRef = ref<InstanceType<typeof ChangesConfirmationModal> | null>(null);

const loading = ref(false);
const form = reactive({
  orgURL: '',
  // user: '',
  // pass: '',
  enableAllRepos: false,
  enableGit: true,
  repoNames: [] as string[],
});

const { hasFormChanged, formSnapshot } = formChangeDetector(form);
const $v = useVuelidate({
  orgURL: { required },
  repoNames: {
    required: (value: string[]) => (form.enableAllRepos ? true : value.length > 0 && value.every((v) => v.trim() !== '')),
  },
}, form, { $stopPropagation: true });

const { doGerritConnect } = mapActions('integration');
const isVisible = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    emit('update:modelValue', value);
  },
});
const logoUrl = gerrit.image;
const showEmptyState = ref(!props.integration?.settings?.remote?.orgURL);

const connectTooltipContent = computed(() => {
  // if edit mode
  if (props.integration?.settings) {
    return hasFormChanged.value
    && $v.value.$invalid ? 'Please enter the organization URL, and at least 1 remote URL, in order to connect Gerrit' : undefined;
  }

  return 'Please enter the organization URL, and at least 1 remote URL, in order to connect Gerrit';
});

const syncData = () => {
  if (props.integration?.settings?.remote) {
    form.orgURL = props.integration?.settings.remote.orgURL;
    form.repoNames = [...(props.integration?.settings.remote.repoNames || [])];
    form.enableAllRepos = props.integration?.settings.remote.enableAllRepos;
  }
  formSnapshot();
};

const revertChanges = () => {
  syncData();
};

onMounted(() => {
  syncData();
});

const addRepoName = () => {
  form.repoNames.push('');
};

const removeRepoName = (index: number) => {
  form.repoNames.splice(index, 1);
};

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

const cancel = () => {
  isVisible.value = false;
};

const connect = async () => {
  loading.value = true;

  const isUpdate = !!props.integration?.settings;

  doGerritConnect({
    orgURL: form.orgURL,
    // user: form.user,
    // pass: form.pass,
    repoNames: form.repoNames,
    enableAllRepos: form.enableAllRepos,
    enableGit: form.enableGit,
    segmentId: props.segmentId,
    grandparentId: props.grandparentId,
    errorHandler,
  })
    .then(() => {
      trackEvent({
        key: isUpdate
          ? FeatureEventKey.EDIT_INTEGRATION_SETTINGS
          : FeatureEventKey.CONNECT_INTEGRATION,
        type: EventType.FEATURE,
        properties: {
          platform: Platform.GERRIT,
        },
      });

      isVisible.value = false;
    })
    .finally(() => {
      loading.value = false;
    });
};

const errorHandler = (error: any) => {
  const errorMessage = error?.response?.data;
  const parsedError = parseDuplicateRepoError(errorMessage, 'There was an error mapping gerrit repositories');

  if (parsedError) {
    const { repo, eId } = parsedError;
    // TODO: This is returning 404 error for some reason. It could be that the data returned by the error is incorrect.
    IntegrationService.find(eId)
      .then((integration) => {
        customRepoErrorMessage(integration.segment, repo, 'gerrit');
      })
      .catch(() => {
        ToastStore.error(errorMessage);
      });
  }
};
</script>

<script lang="ts">
export default {
  name: 'LfGerritSettingsDrawer',
};
</script>
