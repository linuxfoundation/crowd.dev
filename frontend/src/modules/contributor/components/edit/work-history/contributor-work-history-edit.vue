<template>
  <lf-modal v-model="isModalOpen" width="30rem">
    <template #default="{ close }">
      <div class="px-6 pt-4 pb-10">
        <div class="flex items-center justify-between pb-6">
          <h5>{{ isEdit ? 'Edit' : 'Add' }} work experience</h5>
          <lf-button type="secondary-ghost-light" :icon-only="true" @click="close">
            <lf-icon name="xmark" />
          </lf-button>
        </div>

        <div>
          <lf-field label-text="Organization" :required="true" class="mb-5">
            <lf-organization-select
              v-model="form.organization"
              class="w-full"
              :class="$v.organization.$invalid && $v.organization.$dirty ? 'is-error' : ''"
              @blur="$v.organization.$touch"
              @change="$v.organization.$touch"
            />

            <lf-field-messages
              :validation="$v.organization"
              :error-messages="{
                required: 'This field is required',
              }"
            />
            <lf-field-message v-if="hasSameOrganization" type="warning" class="!mt-2">
              There is already a work experience associated with this organization.
              Please ensure that either the Job title or Period is different before adding this work experience.
            </lf-field-message>
          </lf-field>
          <lf-field label-text="Job title" class="mb-5">
            <lf-input v-model="form.title" />
          </lf-field>
          <lf-field label-text="Period" class="mb-3">
            <div class="flex w-full">
              <el-date-picker
                v-model="form.dateStart"
                type="month"
                placeholder="From"
                class="!w-auto custom-date-range-picker date-from -mr-px"
                popper-class="date-picker-popper"
                :class="$v.dateStart.$invalid && $v.dateStart.$dirty ? 'is-error' : ''"
                format="MMM YYYY"
                @blur="$v.dateStart.$touch"
                @change="$v.dateStart.$touch"
              />
              <el-date-picker
                :model-value="form.currentlyWorking ? '' : form.dateEnd"
                type="month"
                :placeholder="!form.dateStart ? 'To' : 'Present'"
                :disabled="form.currentlyWorking"
                class="!w-auto custom-date-range-picker date-to"
                popper-class="date-picker-popper"
                :class="$v.dateStart.$invalid && $v.dateStart.$dirty ? 'is-error' : ''"
                format="MMM YYYY"
                @update:model-value="form.dateEnd = $event"
                @blur="$v.dateStart.$touch"
                @change="$v.dateStart.$touch"
              />
            </div>
            <lf-field-messages
              :validation="$v.dateStart"
              :error-messages="{
                minDate: 'This date range is not valid',
              }"
            />
          </lf-field>
          <lf-checkbox v-model="form.currentlyWorking" size="small">
            Currently working at this organization
          </lf-checkbox>
        </div>
      </div>
      <div class="px-6 pb-10 flex">
        <div class="mt-0.5">
          <lf-icon name="circle-info" :size="16" class="text-gray-600" />
        </div>
        <div class="flex-grow pl-2 text-xs text-gray-500">
          <span class="font-semibold">Activities affiliation</span> with a certain organization is automatically defined
          by the work experience period. Any work history updates won’t override
          manual changes to activities affiliation, both individually or per project over a specific time period.
        </div>
      </div>
      <div class="py-4 px-6 border-t border-gray-100 flex items-center justify-end gap-4">
        <lf-button type="secondary-ghost" @click="close">
          Cancel
        </lf-button>
        <lf-tooltip
          :disabled="!hasSameOrgDetails"
          placement="top-end"
          content="Please enter a different Job title or Period, as there is already a work experience associated with the selected organization."
        >
          <lf-button
            type="primary"
            :loading="sending"
            :disabled="$v.$invalid || sending || hasSameOrgDetails"
            @click="updateWorkExperience()"
          >
            {{ isEdit ? 'Update' : 'Add' }} work experience
          </lf-button>
        </lf-tooltip>
      </div>
    </template>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import {
  computed, onMounted, reactive, ref,
} from 'vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import type { MemberOrganization, Organization } from '@/modules/organization/types/Organization';
import { OrganizationSource } from '@/modules/organization/types/Organization';
import LfField from '@/ui-kit/field/Field.vue';
import LfOrganizationSelect from '@/modules/organization/components/shared/organization-select.vue';

import { ToastStore } from '@/shared/message/notification';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import LfFieldMessage from '@/ui-kit/field-message/FieldMessage.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  modelValue: boolean,
  organization: Organization | null,
  contributor: Contributor,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void}>();

const { createContributorOrganization, updateContributorOrganization } = useContributorStore();
const { trackEvent } = useProductTracking();

const isEdit = computed(() => !!props.organization);

const sending = ref<boolean>(false);

interface ConrtibutorWorkHistoryForm {
  organization: Organization | null,
  title: string;
  dateStart: string | null;
  dateEnd: string | null;
  currentlyWorking: boolean,
}

const form = reactive<ConrtibutorWorkHistoryForm>({
  organization: null,
  title: '',
  dateStart: null,
  dateEnd: null,
  currentlyWorking: false,
});

const minDate = (value: string, rest: ConrtibutorWorkHistoryForm) => {
  const { dateEnd, currentlyWorking } = rest;
  return (
    (!value && !dateEnd && !currentlyWorking)
      || (value && !dateEnd && currentlyWorking)
      || (value && dateEnd && dateHelper(value).isBefore(dateHelper(dateEnd)))
      || (!value && !dateEnd)
  );
};

const rules = {
  organization: {
    required,
  },
  dateStart: {
    minDate,
  },
};

const $v = useVuelidate(rules, form);

const updateWorkExperience = () => {
  const data: Partial<MemberOrganization> = {
    organizationId: (form.organization || props.organization)?.id,
    source: OrganizationSource.UI,
    title: form.title,
    dateStart: form.dateStart ? dateHelper(form.dateStart).startOf('month').format('YYYY-MM-DDTHH:mm:ss.SSS[Z]') : undefined,
    dateEnd: !form.currentlyWorking && form.dateEnd ? dateHelper(form.dateEnd).startOf('month').format('YYYY-MM-DDTHH:mm:ss.SSS[Z]') : undefined,
  };

  if (isEdit.value) {
    trackEvent({
      key: FeatureEventKey.EDIT_WORK_EXPERIENCE,
      type: EventType.FEATURE,
      properties: {
        organization: data,
      },
    });
  } else {
    trackEvent({
      key: FeatureEventKey.ADD_WORK_EXPERIENCE,
      type: EventType.FEATURE,
      properties: {
        organization: data,
      },
    });
  }

  let orgs: Organization[] = [...(props.contributor.organizations || [])];
  if (isEdit.value) {
    orgs = orgs.map((o: Organization) => {
      if (o.id === props.organization?.id
          && o.memberOrganizations?.title === props.organization?.memberOrganizations?.title
          && o.memberOrganizations?.dateStart === props.organization?.memberOrganizations?.dateStart
          && o.memberOrganizations?.dateEnd === props.organization?.memberOrganizations?.dateEnd) {
        return data;
      }
      return o;
    });
  } else {
    orgs.push(data);
  }

  sending.value = true;

  (isEdit.value
    ? updateContributorOrganization(props.contributor.id, props.organization?.memberOrganizations?.id!, data)
    : createContributorOrganization(props.contributor.id, data))
    .then(() => {
      ToastStore.success(`Work experience ${isEdit.value ? 'updated' : 'added'} successfully`);
      isModalOpen.value = false;
    })
    .catch(() => {
      ToastStore.error(`Something went wrong while ${isEdit.value ? 'updating' : 'adding'} a work experience`);
    })
    .finally(() => {
      sending.value = false;
    });
};

const isModalOpen = computed<boolean>({
  get() {
    return props.modelValue;
  },
  set(value: boolean) {
    emit('update:modelValue', value);
  },
});

const hasSameOrganization = computed(() => (props.contributor.organizations || []).some((o: Organization) => o.id === form.organization?.id)
    && (!isEdit.value || form.organization?.id !== props.organization?.id));

const hasSameOrgDetails = computed(() => (props.contributor.organizations || [])
  .some((o: Organization) => {
    // Check for the same organization
    if (o.id !== form.organization?.id) {
      return false;
    }

    //  Check for empty info
    if (!form.title && !form.dateStart && !form.dateEnd) {
      return false;
    }

    // Check if titles matching
    if (form.title !== o.memberOrganizations.title) {
      return false;
    }

    // Check if dates matching
    const start = form.dateStart && o.memberOrganizations.dateStart
      ? dateHelper(form.dateStart).startOf('month').isSame(dateHelper(o.memberOrganizations.dateStart), 'day')
      : form.dateStart === o.memberOrganizations.dateStart;
    const end = !form.currentlyWorking && form.dateEnd && o.memberOrganizations.dateEnd
      ? dateHelper(form.dateEnd).startOf('month').isSame(dateHelper(o.memberOrganizations.dateEnd), 'day')
      : form.dateEnd === o.memberOrganizations.dateEnd;
    return start && end;
  }));

onMounted(() => {
  if (props.organization) {
    form.organization = {
      ...props.organization,
      label: props.organization.displayName,
    };
    form.title = props.organization.memberOrganizations?.title || '';
    form.dateStart = props.organization.memberOrganizations?.dateStart || '';
    form.dateEnd = props.organization.memberOrganizations?.dateEnd || '';
    form.currentlyWorking = !form.dateEnd && !!form.dateStart;
  }
});
</script>

<script lang="ts">
export default {
  name: 'LfContributorEditWorkHistory',
};
</script>

<style lang="scss">
.is-error{
  .el-input__wrapper{
    @apply border-red-500 #{!important};
  }
}

.custom-date-range-picker{
  @apply flex-grow;

  &.date-from{
    .el-input__wrapper{
      @apply rounded-r-none h-10 w-full #{!important};
    }
  }

  &.date-to{
    .el-input__wrapper{
      @apply rounded-l-none h-10 w-full #{!important};
    }
  }
}
</style>
