<template>
  <div class="grid gap-x-12 grid-cols-4 mb-16">
    <div v-if="showHeader">
      <h6>Custom attributes</h6>
      <p class="text-gray-500 text-2xs leading-normal mt-1">
        Add custom data points to enhance the person profile
      </p>
      <lf-button
        type="primary-link"
        size="small"
        class="mt-3"
        @click="() => emit('openDrawer')"
      >
        Manage global attributes
      </lf-button>
    </div>
    <div :class="showHeader ? 'col-span-3' : 'col-span-4'">
      <div class="flex gap-3 border-b h-8 items-center">
        <span
          class="uppercase text-gray-400 text-2xs font-semibold tracking-wide w-1/3"
        >Name</span>
        <span
          class="uppercase text-gray-400 text-2xs font-semibold tracking-wide grow"
        >Value</span>
      </div>
      <div
        v-if="!!customAttributes.length"
        class="custom-attributes-form flex mt-4 mb-2 flex-col gap-4"
      >
        <div
          v-for="(attribute, index) in customAttributes"
          :key="index"
        >
          <div
            v-if="
              index === firstHiddenAttributeIndex
                && firstHiddenAttributeIndex !== -1
            "
            class="text-xs text-gray-400 font-medium mt-4 mb-2"
          >
            Hidden in profile
          </div>
          <div class="flex gap-3">
            <div
              class="w-1/3 flex flex-col gap-1 justify-start mt-0.5"
            >
              <div class="flex items-center leading-tight">
                <div
                  class="text-gray-900 text-xs font-medium mr-2"
                >
                  {{ attribute.label }}
                </div>
                <el-tooltip
                  v-if="getAttributeSourceName(model.attributes[attribute.name])"
                  :content="`Source: ${getAttributeSourceName(model.attributes[attribute.name])}`"
                  placement="top"
                  trigger="hover"
                >
                  <lf-svg name="source" class="h-3 w-3" />
                </el-tooltip>
              </div>
              <span
                class="text-2xs text-gray-500 leading-none"
              >{{ AttributeType[attribute.type.toUpperCase()] }}</span>
            </div>
            <el-form-item class="grow">
              <el-date-picker
                v-if="attribute.type === 'date'"
                v-model="model[attribute.name]"
                :prefix-icon="CalendarIcon"
                :clearable="false"
                class="custom-date-picker"
                popper-class="date-picker-popper"
                type="date"
                value-format="YYYY-MM-DD"
                format="YYYY-MM-DD"
                placeholder="YYYY-MM-DD"
              />
              <el-select
                v-else-if="attribute.type === 'boolean'"
                v-model="model[attribute.name]"
                class="w-full"
                clearable
                placeholder="Select option"
              >
                <el-option
                  key="true"
                  label="True"
                  :value="true"
                  @mouseleave="onSelectMouseLeave"
                />
                <el-option
                  key="false"
                  label="False"
                  :value="false"
                  @mouseleave="onSelectMouseLeave"
                />
              </el-select>

              <app-autocomplete-many-input
                v-else-if="attribute.type === 'multiSelect'"
                v-model="model[attribute.name]"
                :fetch-fn="
                  () => fetchCustomAttribute(attribute.id)
                "
                :create-fn="
                  (value) =>
                    updateCustomAttribute(attribute, value)
                "
                placeholder="Select an option or create one"
                input-class="w-full multi-select-field"
                :create-if-not-found="true"
                :collapse-tags="true"
                :parse-model="true"
                :are-options-in-memory="true"
                :options-limit="10"
              />
              <el-input
                v-else
                v-model="model[attribute.name]"
                :type="attribute.type"
                clearable
              /><template #error>
                <div class="el-form-item__error">
                  Value is required
                </div>
              </template>
            </el-form-item>
            <el-tooltip
              placement="top"
              :content="
                attribute.show
                  ? 'Hide attribute'
                  : 'Show attribute'
              "
            >
              <lf-button
                v-if="!attribute.canDelete"
                type="primary-link"
                size="medium"
                class="w-10 h-10"
                icon-only
                @click="
                  updateAttribute(attribute.id, {
                    show: !attribute.show,
                  })
                "
              >
                <lf-icon
                  :name="attribute.show ? 'eye' : 'eye-slash'"
                  :size="16"
                  class="text-black"
                />
              </lf-button>
            </el-tooltip>
          </div>
        </div>
      </div>
      <div
        v-else
        class="text-gray-500 text-xs italic mt-4 mb-2"
      >
        No attributes available
      </div>
    </div>
  </div>
</template>

<script setup>
import {
  defineProps,
  defineEmits,
  computed,
  h,
  watch,
} from 'vue';
import { onSelectMouseLeave } from '@/utils/select';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';

import { ToastStore } from '@/shared/message/notification';
import { MemberService } from '@/modules/member/member-service';
import LfSvg from '@/shared/svg/svg.vue';
import { AttributeType } from '@/modules/organization/types/Attributes';
import { getAttributeSourceName } from '@/shared/helpers/attribute.helpers';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps({
  attributes: {
    type: Array,
    default: () => [],
  },
  modelValue: {
    type: Object,
    default: () => {},
  },
  showHeader: {
    type: Boolean,
    default: true,
  },
  record: {
    type: Object,
    default: () => {},
  },
});

const emit = defineEmits([
  'update:modelValue',
  'openDrawer',
]);

const CalendarIcon = h(
  'i', // type
  {
    class:
      'fa-calendar fa-light text-base leading-none text-gray-400',
  }, // props
  [],
);

const customAttributes = computed(() => props.attributes
  .filter((attribute) => {
    // For new member form, only display attributes that can be deleted
    if (!props.record) {
      return attribute.canDelete;
    }

    return (
      (![
        'bio',
        'url',
        'location',
        'jobTitle',
        'emails',
        'workExperiences', // we render them in _aside-enriched
        'certifications', // we render them in _aside-enriched
        'education', // we render them in _aside-enriched
        'awards', // we render them in _aside-enriched
      ].includes(attribute.name)
          && props.record.attributes[attribute.name])
        // Global attributes
        || attribute.canDelete
    );
  })
  .sort((a, b) => {
    // For new member form, maintain order
    if (!props.record) {
      return 0;
    }

    const aEnrichment = Number(
      props.record.attributes[a.name]?.enrichment || 0,
    );
    const bEnrichment = Number(
      props.record.attributes[b.name]?.enrichment || 0,
    );

    // Sort order
    // 1. All attributes that have show = true && canDelete = true
    // 2. All attributes that have show = true && canDelete = false
    // 3. All attributes that have show = false && canDelete = false
    // For each of these conditions, the enrichment attributes show at the top
    if (a.show === b.show) {
      if (a.canDelete === b.canDelete) {
        return bEnrichment - aEnrichment;
      }
      return b.canDelete - a.canDelete;
    }
    return b.show - a.show;
  }));

const firstHiddenAttributeIndex = computed(() => customAttributes.value.findIndex(
  (attribute) => !attribute.show,
));
const { doUpdateCustomAttributes } = mapActions('member');
const model = computed(() => props.modelValue);

watch(model.value, (newModel) => {
  emit('update:modelValue', newModel);
});

const updateAttribute = (id, data) => {
  ConfirmDialog({
    type: 'danger',
    title: `${data.show ? 'Show' : 'Hide'} attribute`,
    message: `This attribute will be ${
      data.show ? 'available' : 'hidden'
    } in all member profiles. Are you sure you want to proceed? You can undo this action later.`,
    confirmButtonText: 'Confirm',
    cancelButtonText: 'Cancel',
    icon: data.show ? 'fa-eye fa-light' : 'fa-eye-slash fa-light',
  }).then(() => {
    doUpdateCustomAttributes({ id, data }).then(() => {
      ToastStore.success('Attribute successfully updated');
    });
  });
};

// Get a custom attribute by id and parse the attribute options
// into an object format with id and label
const fetchCustomAttribute = (id) => MemberService.getCustomAttribute(id)
  .then((response) => response.options.sort().map((o) => ({
    id: o,
    label: o,
  })))
  .catch(() => []);

// Create a new option for the custom attribute and
// return the new option in an object format with id and label
const updateCustomAttribute = (attribute, value) => {
  const options = [...attribute.options];

  options.push(value);

  return MemberService.updateCustomAttribute(attribute.id, {
    options,
  }).then(() => ({
    id: value,
    label: value,
  }));
};
</script>

<style lang="scss">
.form-enrichment-badge {
  @apply w-5 h-5 bg-gray-100 rounded-full flex items-center justify-center;
  svg {
    @apply h-4 w-4 overflow-visible flex items-center justify-center leading-none;
  }
}

.multi-select-field .el-select__tags {
  @apply h-7;
}
</style>
