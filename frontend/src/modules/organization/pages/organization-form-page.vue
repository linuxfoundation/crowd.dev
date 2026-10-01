<template>
  <app-page-wrapper :container-class="'col-start-1 col-span-12'">
    <div class="organization-form-page">
      <div class="sticky -top-5 z-20 bg-white -mx-2 px-2 -mt-6 pt-6 block">
        <div class="border-b border-gray-200">
          <lf-button
            key="organizations"
            link
            :icon="ArrowPrevIcon"
            type="secondary-link"
            size="medium"
            class="text-gray-600 p-0"
            @click="onCancel"
          >
            Organizations
          </lf-button>
          <div class="flex justify-between">
            <div class="flex items-center gap-4 mt-4 mb-6">
              <h4>
                {{
                  isEditPage
                    ? 'Edit organization'
                    : 'New organization'
                }}
              </h4>
              <div
                v-if="!isEditPage && selectedSegments.project && selectedSegments.subproject"
                class="badge badge--gray-light badge--xs"
              >
                {{ selectedSegments.subproject.name }} ({{ selectedSegments.project.name }})
              </div>
            </div>
            <div class="flex items-center">
              <lf-button
                v-if="isEditPage && hasFormChanged"
                type="primary-link"
                class="!px-3"
                :disabled="isFormSubmitting"
                @click="onReset"
              >
                <lf-icon name="arrow-turn-left" />
                <span>Reset changes</span>
              </lf-button>
              <div
                v-if="isEditPage && hasFormChanged"
                class="mx-4 border-x border-gray-200 h-10"
              />
              <div class="flex gap-4">
                <lf-button
                  :disabled="isFormSubmitting"
                  type="bordered"
                  size="medium"
                  @click="onCancel"
                >
                  Cancel
                </lf-button>
                <lf-button
                  :disabled="isSubmitBtnDisabled"
                  :loading="isFormSubmitting"
                  :loading-icon="LoaderIcon"
                  type="primary"
                  size="medium"
                  @click="onSubmit"
                >
                  {{
                    isEditPage
                      ? 'Update organization'
                      : 'Add organization'
                  }}
                </lf-button>
              </div>
            </div>
          </div>
        </div>
      </div>

      <el-container
        v-if="!isPageLoading"
        class="bg-white rounded-b-lg flex flex-col"
      >
        <div
          v-if="!isEditPage"
          class="grid gap-x-12 grid-cols-3 bg-gray-50 p-6"
        >
          <div class="col-span-2 col-start-2 relative">
            <app-lf-sub-projects-list-dropdown
              :selected-subproject="selectedSegments.subproject"
              :selected-subproject-parent="selectedSegments.project"
              @on-change="onChange"
            />
          </div>
        </div>
        <el-main class="p-6">
          <el-form
            ref="formRef"
            class="form flex-col"
            label-position="top"
            :rules="rules"
            :model="formModel"
          >
            <app-organization-form-details
              v-model="formModel"
              :fields="fields"
            />
            <el-divider
              class="!mb-6 !mt-8 !border-gray-200"
            />
            <div class="grid gap-x-12 grid-cols-3">
              <h6>Identities</h6>
              <div class="col-span-2">
                <app-organization-form-identities
                  v-model="formModel"
                  :record="record"
                />
              </div>
            </div>
            <el-divider
              class="!mb-6 !mt-8 !border-gray-200"
            />
            <div class="grid gap-x-12 grid-cols-3">
              <h6>Emails</h6>
              <div class="col-span-2">
                <app-organization-form-emails
                  v-model="formModel"
                />
              </div>
            </div>
            <el-divider
              class="!mb-6 !mt-8 !border-gray-200"
            />
            <div v-if="shouldShowAttributes">
              <el-divider class="!mb-6 !mt-8 !border-gray-200" />
              <app-organization-form-attributes
                v-model="formModel"
                :organization="record"
              />
            </div>
          </el-form>
        </el-main>
      </el-container>
      <el-container v-else>
        <div v-loading="isPageLoading" class="app-page-spinner w-full" />
      </el-container>
    </div>
  </app-page-wrapper>
</template>

<script setup>
import {
  computed, h, onMounted, onUnmounted, reactive, ref, watch,
} from 'vue';
import { onBeforeRouteLeave, useRoute, useRouter } from 'vue-router';
import isEqual from 'lodash/isEqual';
import { OrganizationModel } from '@/modules/organization/organization-model';
import { FormSchema } from '@/shared/form/form-schema';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import AppOrganizationFormIdentities from '@/modules/organization/components/form/organization-form-identities.vue';
import AppOrganizationFormDetails from '@/modules/organization/components/form/organization-form-details.vue';
import AppOrganizationFormAttributes from '@/modules/organization/components/form/organization-form-attributes.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { OrganizationService } from '@/modules/organization/organization-service';
import Errors from '@/shared/error/errors';

import { ToastStore } from '@/shared/message/notification';
import enrichmentAttributes from '@/modules/organization/config/enrichment';
import { AttributeType } from '@/modules/organization/types/Attributes';
import AppOrganizationFormEmails from '@/modules/organization/components/form/organization-form-emails.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import AppLfSubProjectsListDropdown from '@/modules/admin/modules/projects/components/lf-sub-projects-list-dropdown.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { useOrganizationStore } from '../store/pinia';

const props = defineProps({
  id: {
    type: String,
    default: null,
  },
});
const LoaderIcon = h(
  'i',
  {
    class: 'fa-circle-notch fa-light text-sm text-white',
  },
  [],
);
const ArrowPrevIcon = h(
  'i', // type
  {
    class: 'fa-chevron-left fa-light text-base leading-none',
  }, // props
  [],
);

const { fields } = OrganizationModel;
const formSchema = new FormSchema([
  fields.name,
  fields.displayName,
  fields.description,
  fields.location,
  fields.employees,
  fields.revenueRange,
  fields.emails,
  fields.identities,
  fields.type,
  fields.size,
  fields.industry,
  fields.founded,
  fields.averageEmployeeTenure,
  fields.averageTenureByLevel,
  fields.averageTenureByRole,
  fields.employeeChurnRate,
  fields.employeeCountByCountry,
  fields.employeeCountByMonth,
  fields.employeeGrowthRate,
  fields.gicsSector,
  fields.grossAdditionsByMonth,
  fields.grossDeparturesByMonth,
  fields.immediateParent,
  fields.ultimateParent,
]);

const { trackEvent } = useProductTracking();

const router = useRouter();
const route = useRoute();

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const organizationsStore = useOrganizationStore();

const selectedSegments = computed(() => {
  let subproject;

  const project = selectedProjectGroup.value.projects.find((p) => p.subprojects.some((sp) => {
    if (sp.id === route.query.subprojectId) {
      subproject = sp;
      return true;
    }

    return false;
  }));

  return {
    project,
    subproject,
  };
});

function getInitialModel(record) {
  return JSON.parse(
    JSON.stringify(
      formSchema.initialValues({
        ...(record || {}),
        name: record ? record.name : '',
        displayName: record ? record.displayName || record.name : '',
        joinedAt: record ? record.joinedAt : '',
        identities: record
          ? [
            ...record.identities,
          ]
          : [],
      }),
    ),
  );
}

const record = ref(null);
const formRef = ref(null);
const formModel = ref(getInitialModel());

const isPageLoading = ref(true);
const isFormSubmitting = ref(false);
const wasFormSubmittedSuccessfully = ref(false);

const rules = reactive(formSchema.rules());

// UI Validations
const isEditPage = computed(() => !!route.params.id);
const isFormValid = computed(() => formSchema.isValidSync(formModel.value));

const segments = computed(() => {
  if (!isEditPage.value) {
    return selectedSegments.value.subproject
      ? [selectedSegments.value.subproject.id]
      : [];
  }

  return record.value.segments?.map((s) => s.id) || [];
});

const hasFormChanged = computed(() => {
  const initialModel = isEditPage.value
    ? getInitialModel(record.value)
    : getInitialModel();

  return !isEqual(initialModel, formModel.value);
});

const isSubmitBtnDisabled = computed(
  () => !isFormValid.value
    || isFormSubmitting.value
    || (isEditPage.value && !hasFormChanged.value),
);

const shouldShowAttributes = computed(() => enrichmentAttributes.some((a) => {
  if (!a.showInForm) {
    return false;
  }

  if (a.type === AttributeType.ARRAY) {
    return !!record.value?.[a.name]?.length;
  }

  return !!record.value?.[a.name];
}));

// Prevent lost data on route change
onBeforeRouteLeave((to) => {
  if (
    hasFormChanged.value
    && !wasFormSubmittedSuccessfully.value
    && to.fullPath !== '/500'
  ) {
    return ConfirmDialog({})
      .then(() => true)
      .catch(() => false);
  }

  return true;
});

onMounted(async () => {
  if (isEditPage.value) {
    const { id } = route.params;
    const { segmentId, projecGroup } = route.query;
    const segments = segmentId || projecGroup ? [segmentId || projecGroup] : null;

    try {
      record.value = await OrganizationService.find(id, segments);
    } catch (error) {
      Errors.handle(error);
      router.push({ name: 'organization' });
    }

    isPageLoading.value = false;
    formModel.value = getInitialModel(record.value);
  } else {
    isPageLoading.value = false;
  }
});

// Prevent window reload when form has changes
const preventWindowReload = (e) => {
  if (hasFormChanged.value) {
    e.preventDefault();
    e.returnValue = '';
  }
};

window.addEventListener('beforeunload', preventWindowReload);

onUnmounted(() => {
  window.removeEventListener('beforeunload', preventWindowReload);
});

// Once form is submitted successfully, update route
watch(wasFormSubmittedSuccessfully, (isFormSubmittedSuccessfully) => {
  if (isFormSubmittedSuccessfully) {
    if (isEditPage.value) {
      const { segmentId, projectGroup } = route.query;

      return router.push({
        name: 'organizationView',
        params: {
          id: record.value.id,
        },
        query: {
          segmentId: segmentId || projectGroup,
        },
      });
    }

    return router.push({ name: 'organization' });
  }
  return null;
});

function onReset() {
  formModel.value = isEditPage.value
    ? getInitialModel(record.value)
    : getInitialModel();
}

function onCancel() {
  const { segmentId, projectGroup } = route.query;

  router.push({
    name: 'organizationView',
    params: {
      id: record.value.id,
    },
    query: {
      segmentId: segmentId || projectGroup,
    },
  });
}

async function onSubmit() {
  isFormSubmitting.value = true;

  const { emails, ...rest } = formModel.value;

  const name = isEditPage.value === false ? formModel.value.displayName : undefined;

  const data = {
    manuallyCreated: true,
    ...rest,
    attributes: {
      name: {
        default: name,
        custom: [name],
      },
    },
  };

  const payload = isEditPage.value
    ? {
      id: props.id,
      values: data,
      segments: segments.value,
    }
    : {
      ...data,
      segments: segments.value,
    };

  // Edit
  if (isEditPage.value) {
    trackEvent({
      key: FeatureEventKey.EDIT_ORGANIZATION,
      type: EventType.FEATURE,
      properties: {
        ...payload.values,
      },
    });

    try {
      await OrganizationService.update(payload.id, payload.values);
      ToastStore.success('Organization successfully saved');
    } catch (error) {
      if (error.response.status === 409) {
        ToastStore.error(
          h(
            'div',
            {
              class: 'flex flex-col gap-2',
            },
            [
              h(
                'button',
                {
                  class: 'c-btn c-btn--tiny c-btn--secondary-gray !h-6 !w-fit',
                  onClick: () => {
                    organizationsStore.addToMergeOrganizations(payload.id, error.response.data);
                    ToastStore.closeAll();
                  },
                },
                'Merge organizations',
              ),
            ],
          ),
          {
            title: 'Organization was not updated because the website already exists in another organization.',
          },
        );
      } else {
        Errors.handle(error);
      }
    }
  } else {
    trackEvent({
      key: FeatureEventKey.ADD_ORGANIZATION,
      type: EventType.FEATURE,
      properties: {
        ...payload,
      },
    });

    // Create
    try {
      await OrganizationService.create(payload);

      ToastStore.success('Organization successfully saved');
    } catch (error) {
      ToastStore.error('There was an error creating the organization');
      Errors.handle(error);
    }
  }
  isFormSubmitting.value = false;
  wasFormSubmittedSuccessfully.value = true;
}

const onChange = ({ subprojectId }) => {
  router.replace({
    name: 'organizationCreate',
    query: {
      subprojectId,
    },
  });
};
</script>

<script>
export default {
  name: 'OrganizationFormPage',
};
</script>

<style lang="scss">
.organization-form-page {

  .el-main {
    @apply max-h-fit;
  }

  // Personal Details form
  .organization-details-form {
    & .el-form-item {
      @apply mb-6;
    }
  }

  .identities-form .el-form-item,
  .custom-attributes-form .el-form-item {
    @apply mb-0;
  }

  .el-form .el-form-item__content,
  .el-form--default.el-form--label-top,
  .custom-attributes-form,
  .el-form-item__content {
    @apply flex mb-0;
  }
}
</style>
