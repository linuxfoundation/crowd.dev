<template>
  <lf-modal
    v-model="isModalOpen"
    width="30rem"
    @close="reset()"
  >
    <!-- header -->
    <section class="px-6 pt-4 pb-6 flex justify-between items-center">
      <div class="flex items-center gap-3">
        <lf-icon name="circle-exclamation" class="text-red-500" :size="24" />
        <h5>Report data issue</h5>
      </div>
      <lf-button
        type="secondary-ghost-light"
        icon-only
        @click="isModalOpen = false;reset()"
      >
        <lf-icon name="xmark" />
      </lf-button>
    </section>

    <section class="px-6 pb-5">
      <article v-if="props.type" class="pb-8">
        <div class="border border-gray-200  rounded-lg">
          <div class="flex items-center h-11 px-4">
            <div class="w-1/3">
              <p class="text-medium font-semibold">
                Data type
              </p>
            </div>
            <div class="w-2/3">
              <p class="text-medium">
                {{ props.type }}
              </p>
            </div>
          </div>
          <div v-if="!!reportDataTypeDisplay[props.type].display" class="flex items-center border-t border-gray-200 px-4 h-11">
            <div class="w-1/3">
              <p class="text-medium font-semibold">
                Attribute
              </p>
            </div>
            <div class="w-2/3">
              <component
                :is="reportDataTypeDisplay[props.type].display"
                :attribute="props.attribute"
                :entity="props.contributor || props.organization"
              />
            </div>
          </div>
        </div>
      </article>
      <article v-if="!props.type" class="pb-5">
        <lf-field label-text="Data issue">
          <div class="flex items-center gap-5 pt-1">
            <lf-radio v-model="form.issue" size="small" value="incorrect" name="issue">
              Incorrect data
            </lf-radio>
            <lf-radio v-model="form.issue" size="small" value="missing" name="issue">
              Missing data
            </lf-radio>
          </div>
        </lf-field>
      </article>
      <article v-if="!props.type" class="pb-5">
        <lf-field label-text="Data type" required>
          <el-select
            v-model="form.type"
            placeholder="Select option"
            class="w-full"
            :invalid="$v.type.$invalid && $v.type.$dirty"
            @blur="$v.type.$touch"
            @change="$v.type.$touch"
          >
            <el-option
              v-for="option of getConfig.types"
              :key="option"
              :label="option"
              :value="option"
            />
          </el-select>
          <lf-field-messages :validation="$v.type" :error-messages="{ required: 'Please select data type' }" />
        </lf-field>
      </article>
      <article class="pb-5">
        <lf-field
          label-text="Description"
          required
          description="Please identity and describe which data attribute is incorrect or missing from this profile."
        >
          <lf-textarea
            v-model="form.description"
            class="min-h-30 w-full"
            :invalid="$v.description.$invalid && $v.description.$dirty"
            @blur="$v.description.$touch"
            @change="$v.description.$touch"
          />
          <lf-field-messages :validation="$v.description" :error-messages="{ required: 'Please enter description' }" />
        </lf-field>
      </article>
    </section>

    <!-- footer -->
    <section class="px-6 py-4 flex justify-end gap-4 border-t border-gray-100">
      <lf-button type="secondary-ghost" @click="isModalOpen = false;reset()">
        Cancel
      </lf-button>
      <lf-button
        :disabled="$v.$invalid"
        :loading="loading"
        @click="submit()"
      >
        Report issue
      </lf-button>
    </section>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import { computed, reactive, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import LfRadio from '@/ui-kit/radio/Radio.vue';
import LfField from '@/ui-kit/field/Field.vue';
import LfTextarea from '@/ui-kit/textarea/Textarea.vue';
import type { Organization } from '@/modules/organization/types/Organization';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import { ReportDataEntity } from '@/shared/modules/report-issue/constants/report-data-entity.enum';
import type { ReportDataConfig } from '@/shared/modules/report-issue/config';
import { reportDataConfig, reportDataTypeDisplay } from '@/shared/modules/report-issue/config';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import type { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';
import authAxios from '@/shared/axios/auth-axios';

import { ToastStore } from '@/shared/message/notification';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  modelValue: boolean,
  type?: ReportDataType,
  attribute?: any,
  contributor?: Contributor,
  organization?: Organization,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void}>();

const loading = ref<boolean>(false);

const form = reactive({
  issue: 'incorrect',
  type: props.type || '',
  description: '',
});

const rules = {
  issue: {
    required,
  },
  type: {
    required,
  },
  description: {
    required,
  },
};

const $v = useVuelidate(rules, form);

const isModalOpen = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
});

const getType = computed(() => {
  if (props.contributor) {
    return ReportDataEntity.PERSON;
  }
  return ReportDataEntity.ORGANIZATION;
});

const entity = computed(() => props.contributor || props.organization);

const getConfig = computed<ReportDataConfig>(() => reportDataConfig[getType.value]);

const reset = () => {
  form.issue = 'incorrect';
  form.type = '';
  form.description = '';
  $v.value.$reset();
};

const submit = () => {
  const typeDescription = props.type ? reportDataTypeDisplay[props.type].description(props.attribute, entity.value) : '';
  const description = `${typeDescription.length ? `${typeDescription}\n\n` : ''}${form.description}`;
  const data = {
    profileUrl: window.location.href,
    dataIssue: form.issue,
    dataType: form.type,
    description,
  };

  loading.value = true;
  return authAxios.post(
    `${getConfig.value.url(entity.value.id)}`,
    data,
  )
    .then(() => {
      isModalOpen.value = false;
      reset();
      ToastStore.success('Thanks for reporting this data issue! Our team will address it as soon as possible and inform you once it is resolved.', {
        title: 'Data issue reported successfully',
      });
    })
    .catch(() => {
      ToastStore.error('Something went wrong while reporting data issue');
    })
    .finally(() => {
      loading.value = false;
    });
};
</script>

<script lang="ts">
export default {
  name: 'LfReportDataIssueModal',
};
</script>
