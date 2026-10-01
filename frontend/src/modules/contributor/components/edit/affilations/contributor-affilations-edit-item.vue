<template>
  <div class="flex w-full gap-3">
    <div class="w-5/12">
      <el-select
        v-model="form.organization"
        filterable
        class="w-full grow"
        placeholder="Select organization"
      >
        <template v-if="form && (form.organization)" #prefix="{}">
          <div class="flex items-center">
            <lf-avatar
              :name="selectedOrg?.name"
              :src="selectedOrg?.logo"
              :size="20"
              class="!rounded-sm"
            >
              <template #placeholder>
                <div class="w-full h-full bg-gray-50 flex items-center justify-center">
                  <lf-icon name="house-building" :size="12" class="text-gray-400" />
                </div>
              </template>
            </lf-avatar>
          </div>
        </template>
        <el-option
          v-for="organization in availableOrganizations"
          :key="organization.id"
          :label="organization.name"
          :value="organization.id"
          class="!px-3"
        >
          <div class="flex gap-2 items-center">
            <lf-avatar
              v-if="organization.id"
              :name="organization.name"
              :src="organization.logo"
              :size="20"
              class="!rounded-sm"
            >
              <template #placeholder>
                <div class="w-full h-full bg-gray-50 flex items-center justify-center">
                  <lf-icon name="house-building" :size="12" class="text-gray-400" />
                </div>
              </template>
            </lf-avatar>
            <div>{{ organization.name }}</div>
          </div>
        </el-option>
      </el-select>
    </div>
    <div class="w-7/12">
      <div class="flex items-center">
        <el-date-picker
          :model-value="!form.organization ? '' : form.dateStart"
          type="month"
          placeholder="From"
          class="!w-auto custom-date-range-picker date-from -mr-px"
          popper-class="date-picker-popper"
          :class="$v.dateStart.$invalid && $v.dateStart.$dirty ? 'is-error' : ''"
          format="MMM YYYY"
          :disabled="!form.organization"
          @update:model-value="form.dateStart = $event"
          @blur="$v.dateStart.$touch"
          @change="$v.dateStart.$touch"
        />
        <el-date-picker
          :model-value="!form.organization || form.currentlyAffiliated ? '' : form.dateEnd"
          type="month"
          :placeholder="!form.currentlyAffiliated ? 'To' : 'Present'"
          :disabled="form.currentlyAffiliated || !form.organization"
          class="!w-auto custom-date-range-picker date-to"
          popper-class="date-picker-popper"
          :class="$v.dateStart.$invalid && $v.dateStart.$dirty ? 'is-error' : ''"
          format="MMM YYYY"
          @update:model-value="form.dateEnd = $event"
          @blur="$v.dateStart.$touch"
          @change="$v.dateStart.$touch"
        />
        <div>
          <slot />
        </div>
      </div>

      <lf-field-messages
        :validation="$v.dateStart"
        :error-messages="{
          required: 'This field is required',
          minDate: 'This date range is not valid',
        }"
      />

      <lf-checkbox
        v-model="form.currentlyAffiliated"
        size="small"
        class="mt-2"
        :disabled="!form.organization"
      >
        <span class="text-gray-500">Currently affiliated with this organization</span>
      </lf-checkbox>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import useVuelidate from '@vuelidate/core';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import { required } from '@vuelidate/validators';
import { dateHelper } from '@/shared/date-helper/date-helper';

export interface AffilationForm {
  segmentId: string;
  organization: string | null,
  dateStart: string;
  dateEnd: string;
  currentlyAffiliated: boolean;
}

const props = defineProps<{
  modelValue: AffilationForm,
  contributor: Contributor,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: AffilationForm): void }>();

const { displayName, logo } = useOrganizationHelpers();

const availableOrganizations = computed(() => [
  ...props.contributor.organizations.map((organization) => ({
    id: organization.id,
    name: displayName(organization),
    logo: logo(organization),
  })),
]);

const form = computed<AffilationForm>({
  get() {
    return props.modelValue;
  },
  set(value: AffilationForm) {
    emit('update:modelValue', value);
  },
});

const minDate = (value: string, rest: AffilationForm) => {
  const { dateEnd, currentlyAffiliated } = rest;
  return (
    (!value && !dateEnd && !currentlyAffiliated)
      || (value && !dateEnd && currentlyAffiliated)
      || (value && dateEnd && dateHelper(value).isBefore(dateHelper(dateEnd)))
      || (!value && !dateEnd)
  );
};

const rules = {
  organization: {
    required,
  },
  dateStart: {
    required,
    minDate,
  },
};

const $v = useVuelidate(rules, form);

const selectedOrg = computed(() => availableOrganizations.value.find((org) => org.id === form.value.organization));
</script>

<script lang="ts">
export default {
  name: 'LfContributorEditAffilationsItem',
};
</script>
