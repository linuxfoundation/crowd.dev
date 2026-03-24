<template>
  <app-drawer
    v-model="isVisible"
    custom-class="integration-reddit-drawer"
    title="Groups.io"
    size="600px"
    pre-title="Integration"
    :show-footer="true"
    has-border
    close-on-click-modal="true"
    :close-function="canClose"
    @close="handleCancel()"
  >
    <template #beforeTitle>
      <img class="min-w-6 h-6 mr-2" :src="logoUrl" alt="Groups.io logo" />
    </template>
    <template #belowTitle>
      <drawer-description integration-key="groupsio" />
    </template>
    <template #content>
      <div class="w-full flex flex-col mb-6">
        <p class="text-[16px] font-semibold">
          Authentication
        </p>
        <div class="text-2xs text-gray-500 leading-normal mb-1">
          Connect a Groups.io account. You must be a group owner to
          authenticate.
        </div>
      </div>
      <el-form label-position="top" class="form" @submit.prevent>
        <app-form-item
          v-if="!isAPIConnectionValid"
          class="mb-6"
          :validation="$v.email"
          label="Email"
          :required="true"
          :error-messages="{
            required: 'This field is required',
            url: 'Enter valid email',
          }"
        >
          <el-input
            ref="focus"
            v-model="form.email"
            type="email"
            @blur="onBlurEmail()"
          />
        </app-form-item>
        <app-form-item
          v-if="!isAPIConnectionValid"
          class="mb-6"
          :validation="$v.apiKey"
          label="Password"
          :required="true"
          :error-messages="{
            required: 'This field is required',
          }"
        >
          <el-input
            ref="focus"
            v-model="form.password"
            :type="'password'"
            @blur="onBlurPassword()"
          >
            <template #suffix>
              <div
                v-if="isValidating"
                v-loading="isValidating"
                class="flex items-center justify-center w-6 h-6"
              />
            </template>
          </el-input>
          <div class="flex items-baseline text-tiny text-gray-400 mt-1">
            <lf-icon name="circle-info" class="mr-1" />
            If you signed up for Groups.io using Google login, you may not have a password set.<br />
            To use this integration, you’ll need to set a password in your Groups.io account settings.
          </div>
        </app-form-item>
        <app-form-item
          v-if="!isAPIConnectionValid"
          class="mb-6"
          :validation="$v.twoFactorCode"
          label="2FA Code (optional)"
        >
          <el-input
            ref="focus"
            v-model="form.twoFactorCode"
            type="password"
            @blur="onBlurTwoFactorCode()"
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

        <div class="flex flex-row gap-2">
          <lf-button
            v-if="!isAPIConnectionValid"
            type="secondary-gray"
            size="medium"
            :disabled="!isVerificationEnabled"
            :loading="isVerifyingAccount"
            @click="validateAccount()"
          >
            Verify Account
          </lf-button>

          <lf-button
            v-if="isAPIConnectionValid"
            type="secondary-gray"
            size="medium"
            @click="reverifyAccount()"
          >
            Reverify Account
          </lf-button>

          <div v-if="accountVerificationFailed" class="mt-1">
            <lf-icon
              name="circle-exclamation"
              :size="14"
              class="text-red-500 mr-2"
            />
            <span class="text-red-500 text-sm">{{ errorMessage || "Authentication failed" }}</span>
          </div>
        </div>
      </el-form>
      <div
        class="w-full flex flex-col mb-6 mt-4"
        :class="{ 'opacity-50': !isAPIConnectionValid }"
      >
        <p class="text-[16px] font-semibold">
          Track Groups
        </p>
        <div class="text-2xs text-gray-500 leading-normal mb-1">
          Select which groups and/or subgroups you want to track. All topics
          available in each group will be monitored.
          <a
            href="https://docs.linuxfoundation.org/lfx/community-management/integrations/groups.io"
            target="_blank"
            rel="noopener noreferrer"
            class="hover:underline"
          >Read more</a>
        </div>
      </div>

      <div
        v-for="(group, index) in Object.entries(userSubscriptions)"
        :key="index"
        class="mb-4 text-sm w-full"
      >
        <div
          class="flex justify-between items-center mb-2 bg-gray-50 py-2 px-4 border-b border-t border-gray-200 w-full"
        >
          <div class="flex items-center">
            <p>
              {{ group[0] }}
            </p>
          </div>
          <div class="flex items-center">
            <el-tooltip placement="top" effect="dark">
              <template #content>
                <div class="text-center">
                  Everything on this group will be automatically synced. This
                  applies to all current and future subgroups.
                </div>
              </template>
              <lf-switch
                v-model="group[1].allSubgroupsSelected"
                class="ml-4"
                @change="toggleAllSubgroups(group[1])"
              />
            </el-tooltip>
            <span class="ml-2 text-sm">Auto-Sync</span>
          </div>
        </div>
        <div class="ml-6">
          <div class="flex items-center gap-2">
            <el-tooltip
              v-if="group[1].allSubgroupsSelected"
              placement="top"
              effect="dark"
              content="Disable Auto-Sync in order to remove subgroup"
            >
              <lf-checkbox
                v-model="group[1].mainGroup.selected"
                size="large"
                class="mr-4"
                :disabled="group[1].allSubgroupsSelected"
                @change="updateSelectedGroups(group[1])"
              >
                {{ group[1].mainGroup.group_name }}
              </lf-checkbox>
            </el-tooltip>
            <lf-checkbox
              v-else
              v-model="group[1].mainGroup.selected"
              size="large"
              class="mr-4"
              :disabled="group[1].allSubgroupsSelected"
              @change="updateSelectedGroups(group[1])"
            >
              {{ group[1].mainGroup.group_name }}
            </lf-checkbox>
          </div>
          <div
            v-for="subGroup in group[1].subGroups"
            :key="subGroup.group_name"
            class="flex items-center mt-2 gap-2"
          >
            <el-tooltip
              v-if="group[1].allSubgroupsSelected"
              placement="top"
              effect="dark"
              content="Disable Auto-Sync in order to remove subgroup"
            >
              <lf-checkbox
                v-model="subGroup.selected"
                size="large"
                class="mr-4"
                :disabled="group[1].allSubgroupsSelected"
                @change="updateSelectedGroups(group[1])"
              >
                {{ subGroup.group_name }}
                <span class="text-gray-400 text-xs">Subgroup</span>
              </lf-checkbox>
            </el-tooltip>
            <lf-checkbox
              v-else
              v-model="subGroup.selected"
              size="large"
              class="mr-4"
              :disabled="group[1].allSubgroupsSelected"
              @change="updateSelectedGroups(group[1])"
            >
              {{ subGroup.group_name }}
              <span class="text-gray-400 text-xs">Subgroup</span>
            </lf-checkbox>
          </div>
        </div>
      </div>
    </template>

    <template #footer>
      <drawer-footer-buttons
        :integration="props.integration"
        :is-edit-mode="!!integration?.settings?.email"
        :has-form-changed="hasFormChanged"
        :is-loading="loading"
        :is-submit-disabled="cantConnect || !hasSelectedGroups"
        :cancel="handleCancel"
        :revert-changes="revertChanges"
        :connect="connect"
      />
    </template>
  </app-drawer>
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script setup>
import {
  ref, reactive, onMounted, computed,
} from 'vue';
import { required, email } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import groupsio from '@/config/integrations/groupsio/config';
import AppDrawer from '@/shared/drawer/drawer.vue';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import AppFormItem from '@/shared/form/form-item.vue';
import formChangeDetector from '@/shared/form/form-change';
import { IntegrationService } from '@/modules/integration/integration-service';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';

const { doGroupsioConnect } = mapActions('integration');

const props = defineProps({
  integration: {
    type: Object,
    default: null,
  },
  modelValue: {
    type: Boolean,
    default: false,
  },
  segmentId: {
    type: String,
    default: null,
  },
  grandparentId: {
    type: String,
    default: null,
  },
});

const form = reactive({
  email: '',
  password: '',
  twoFactorCode: '',
  groups: [
    {
      slug: '',
    },
  ],
  groupsValidationState: [null],
});

const { trackEvent } = useProductTracking();
const changesConfirmationModalRef = ref(null);

const isValidating = ref(false);
const isVerificationEnabled = ref(false);
const isVerifyingAccount = ref(false);
const isAPIConnectionValid = ref(false);
const accountVerificationFailed = ref(false);
const errorMessage = ref('');
const cookie = ref('');
const cookieExpiry = ref('');
const loading = ref(false);
const isLoadingSubscriptions = ref(false);
const userSubscriptions = ref([]);

const cantConnect = computed(() => {
  if (props.integration?.settings?.email) {
    return (
      $v.value.$invalid
      || !hasFormChanged.value
      || loading.value
      || form.groups.includes('')
    );
  }
  return (
    $v.value.$invalid
    || !hasFormChanged.value
    || loading.value
    || !isAPIConnectionValid.value
    || form.groups.length === 0
  );
});

const hasSelectedGroups = computed(() => Object.values(userSubscriptions.value).some(
  (group) => group.mainGroup.selected
      || group.subGroups.some((subGroup) => subGroup.selected),
));

const rules = computed(() => {
  if (!props.integration?.settings?.email) {
    return {
      email: {
        required,
        email,
      },
      password: {
        required,
      },
    };
  }
  return {};
});

const $v = useVuelidate(rules, form, { $stopPropagation: true });

const validateAccount = async () => {
  isVerifyingAccount.value = true;
  accountVerificationFailed.value = false;
  errorMessage.value = '';
  try {
    const response = await IntegrationService.groupsioGetToken(
      form.email,
      form.password,
      form.twoFactorCode,
      [props.segmentId],
    );
    const { groupsioCookie, groupsioCookieExpiry } = response;
    cookie.value = groupsioCookie;
    cookieExpiry.value = groupsioCookieExpiry;
    isAPIConnectionValid.value = true;
    getUserSubscriptions();
  } catch (error) {
    isAPIConnectionValid.value = false;
    accountVerificationFailed.value = true;

    // Only display API error message for status code 400
    if (error?.response?.status === 400 && typeof error?.response?.data === 'string') {
      errorMessage.value = error.response.data;
    } else {
      errorMessage.value = 'Authentication failed';
    }
  }
  isVerifyingAccount.value = false;
};

const getGroupsHierarchy = (groups) => {
  const hierarchy = {};

  groups.forEach((group) => {
    const [mainGroupSlug, subGroupSlug] = group.group_name.split('+');

    if (!hierarchy[mainGroupSlug]) {
      hierarchy[mainGroupSlug] = {
        mainGroup: null,
        subGroups: [],
        allSubgroupsSelected: false,
      };
    }

    if (subGroupSlug) {
      hierarchy[mainGroupSlug].subGroups.push({ ...group, selected: false });
    } else {
      hierarchy[mainGroupSlug].mainGroup = { ...group, selected: false };
    }
  });

  return hierarchy;
};

const getUserSubscriptions = async () => {
  isLoadingSubscriptions.value = true;
  try {
    const response = await IntegrationService.groupsioGetUserSubscriptions(
      cookie.value,
    );
    userSubscriptions.value = getGroupsHierarchy(response);
  } catch (error) {
    console.error('Error fetching user subscriptions:', error);
  } finally {
    isLoadingSubscriptions.value = false;
  }
};

const reverifyAccount = () => {
  isAPIConnectionValid.value = false;
  // clear data
  cookie.value = '';
  cookieExpiry.value = '';
  form.email = '';
  form.password = '';
  form.twoFactorCode = '';
  userSubscriptions.value = [];
  $v.value.$reset();
};

const onBlurEmail = () => {
  $v.value.email?.$touch();
  canVerify();
};

const onBlurPassword = () => {
  $v.value.password?.$touch();
  canVerify();
};

const onBlurTwoFactorCode = () => {
  $v.value.twoFactorCode?.$touch();
  canVerify();
};

const canVerify = async () => {
  const isCorrect = await $v.value.$validate();
  if (isCorrect) {
    isVerificationEnabled.value = true;
  } else {
    isVerificationEnabled.value = false;
  }
};

const emit = defineEmits(['update:modelValue']);

const { hasFormChanged, formSnapshot } = formChangeDetector(form);

const logoUrl = groupsio.image;

const isVisible = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    return emit('update:modelValue', value);
  },
});

const canClose = (done) => {
  if (hasFormChanged.value) {
    changesConfirmationModalRef.value?.open().then((discardChanges) => {
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
  formSnapshot();
  emit('update:modelValue', false);
  if (!props.integration?.settings?.email) {
    form.email = '';
    form.password = '';
    form.twoFactorCode = '';
    form.groups = [{}];
    form.groupsValidationState = new Array(form.groups.length).fill(true);
    cookie.value = '';
    isAPIConnectionValid.value = false;
    isVerifyingAccount.value = false;
    accountVerificationFailed.value = false;
    errorMessage.value = '';
    $v.value.$reset();
  } else {
    revertChanges();
  }
  userSubscriptions.value = [];
};

const revertChanges = () => {
  form.email = props.integration?.settings?.email;
  form.password = '';
  form.twoFactorCode = '';
  form.groups = props?.integration?.settings?.groups
    ? [...(props.integration?.settings.groups || [])]
    : [{}];
  form.groupsValidationState = new Array(form.groups.length).fill(true);
  cookie.value = props?.integration?.settings?.token;
  isAPIConnectionValid.value = true;
  isVerifyingAccount.value = false;
  accountVerificationFailed.value = false;
  errorMessage.value = '';
  $v.value.$reset();
};

onMounted(() => {
  if (props.integration?.settings?.email) {
    form.email = props?.integration?.settings?.email;
    // eslint-disable-next-line no-unsafe-optional-chaining
    form.groups = [...props.integration?.settings?.groups];
    form.groupsValidationState = new Array(form.groups.length).fill(true);
    cookie.value = props?.integration?.settings?.token;
    isAPIConnectionValid.value = true;
  }
  formSnapshot();
});

const connect = async () => {
  loading.value = true;

  const isUpdate = !!props.integration?.settings?.email;
  const selectedGroups = Object.values(userSubscriptions.value).flatMap(
    (group) => {
      const selected = [];
      if (group.mainGroup.selected) {
        selected.push({
          id: group.mainGroup.id,
          slug: group.mainGroup.group_name,
          name: group.mainGroup.nice_group_name,
          groupAddedOn: new Date(),
        });
      }
      selected.push(
        ...group.subGroups
          .filter((subGroup) => subGroup.selected)
          .map((subGroup) => ({
            id: subGroup.id,
            slug: subGroup.group_name,
            name: subGroup.nice_group_name,
            groupAddedOn: new Date(),
          })),
      );
      return selected;
    },
  );

  const autoImports = Object.entries(userSubscriptions.value)
    .filter(([_, object]) => object.allSubgroupsSelected)
    .map(([group]) => ({
      mainGroup: group,
      isAllowed: true,
    }));

  doGroupsioConnect({
    email: form.email,
    token: cookie.value,
    tokenExpiry: cookieExpiry.value,
    password: form.password,
    groups: selectedGroups,
    autoImports,
    isUpdate,
    segmentId: props.segmentId,
    grandparentId: props.grandparentId,
  })
    .then(() => {
      trackEvent({
        key: isUpdate
          ? FeatureEventKey.EDIT_INTEGRATION_SETTINGS
          : FeatureEventKey.CONNECT_INTEGRATION,
        type: EventType.FEATURE,
        properties: {
          platform: Platform.GROUPS_IO,
        },
      });

      isVisible.value = false;
    })
    .finally(() => {
      loading.value = false;
    });

  formSnapshot();
};

const toggleAllSubgroups = (group) => {
  if (group.allSubgroupsSelected) {
    // eslint-disable-next-line no-param-reassign
    group.mainGroup.selected = true;
    group.subGroups.forEach((subGroup) => {
      // eslint-disable-next-line no-param-reassign
      subGroup.selected = true;
    });
  }
  // If toggle is turned off, we don't change the state of subgroups
};

const updateSelectedGroups = (group) => {
  const allSelected = group.mainGroup.selected
    && group.subGroups.every((subGroup) => subGroup.selected);
  if (!allSelected) {
    // eslint-disable-next-line no-param-reassign
    group.allSubgroupsSelected = false;
  }
};
</script>

<script>
export default {
  name: 'LfGroupsioSettingsDrawer',
};
</script>

<style scoped>
/* Add this style block */
:deep(.c-checkbox input:disabled:checked) {
  --lf-checkbox-background: var(--lf-color-primary-200);
  /* Muted blue background */
  --lf-checkbox-border: var(--lf-color-primary-200);
  /* Muted blue border */
}
</style>
