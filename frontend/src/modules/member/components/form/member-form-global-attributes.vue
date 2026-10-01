<template>
  <div
    class="flex flex-col items-start justify-between h-full"
  >
    <div class="w-full px-6 pb-6 overflow-auto">
      <div class="flex gap-4 border-b h-8 items-center">
        <div
          class="attribute-type uppercase text-gray-400 text-2xs font-semibold tracking-wide"
        >
          Type
        </div>
        <div
          class="uppercase text-gray-400 text-2xs font-semibold tracking-wide"
        >
          Name <span class="text-red-500">*</span>
        </div>
      </div>
      <el-form :model="model">
        <div
          v-if="!!Object.keys(model).length"
          class="flex flex-col gap-4 mt-4"
        >
          <div
            v-for="(attribute, index) in model"
            :key="index"
            class="flex gap-4"
          >
            <el-form-item class="attribute-type">
              <el-select
                v-model="attribute.type"
                :disabled="attribute.canDelete"
                popper-class="attribute-popper-class"
                placeholder="Type"
                size="large"
              >
                <el-option
                  v-for="typeOption in attributeTypes"
                  :key="typeOption.value"
                  :label="typeOption.label"
                  :value="typeOption.value"
                  @mouseleave="onSelectMouseLeave"
                />
              </el-select>
            </el-form-item>
            <el-form-item
              :prop="`${attribute.name}.label`"
              required
              class="grow"
            >
              <el-input
                v-model="attribute.label"
                @input="(e) => onInputChange(e, attribute)"
              /><template #error>
                <div class="el-form-item__error">
                  Name is required
                </div>
              </template>
            </el-form-item>
            <lf-button
              type="primary-link"
              size="medium"
              class="w-10 h-10"
              icon-only
              @click="deleteAttribute(attribute.name)"
            >
              <lf-icon name="trash-can" :size="20" class="text-black" />
            </lf-button>
          </div>
        </div>
      </el-form>
      <lf-button
        type="primary-link"
        size="medium"
        class="mt-5"
        @click="addAttribute"
      >
        + Add attribute
      </lf-button>
    </div>

    <div class="el-drawer__footer">
      <div
        class="flex w-full justify-end"
        :class="
          hasFormChanged ? 'justify-between' : 'justify-end'
        "
      >
        <lf-button
          v-if="hasFormChanged"
          type="primary-link"
          size="medium"
          @click="onReset"
        >
          <lf-icon name="arrow-turn-left" />
          <span>Reset changes</span>
        </lf-button>
        <div class="flex gap-4">
          <lf-button
            type="secondary-gray"
            size="medium"
            @click="() => (isDrawerOpen = false)"
          >
            Cancel
          </lf-button>
          <lf-button
            :disabled="!hasFormChanged || isFormInvalid"
            type="primary"
            size="medium"
            @click="onSubmit"
          >
            Update
          </lf-button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import {
  defineProps,
  defineEmits,
  computed,
  reactive,
  ref,
  watch,
} from 'vue';
import { useStore } from 'vuex';
import isEqual from 'lodash/isEqual';
import cloneDeep from 'lodash/cloneDeep';
import attributeTypes from '@/jsons/member-custom-attributes.json';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';

import { ToastStore } from '@/shared/message/notification';
import parseCustomAttributes from '@/shared/fields/parse-custom-attributes';
import { onSelectMouseLeave } from '@/utils/select';
import { useMemberStore } from '@/modules/member/store/pinia';
import { storeToRefs } from 'pinia';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: () => false,
  },
});
const emit = defineEmits(['update:modelValue']);
const { trackEvent } = useProductTracking();

const store = useStore();
const memberStore = useMemberStore();
const { customAttributes } = storeToRefs(memberStore);

// Arrays used to make the requests on form submission
const editedFields = reactive([]);
const addedFields = reactive([]);
const deletedFields = reactive([]);

// Form models
const initialModel = ref(
  cloneDeep(
    parseCustomAttributes(
      customAttributes.value,
    ),
  ),
);
// eslint-disable-next-line vue/no-ref-object-reactivity-loss -- intentional initial snapshot
const model = ref(cloneDeep(initialModel.value));

const isFormInvalid = computed(() => Object.entries(model.value).some(
  ([, value]) => !value.label,
));
const hasFormChanged = computed(() => !isEqual(initialModel.value, model.value));
const isDrawerOpen = computed({
  get() {
    return props.modelValue;
  },
  set(drawerVisibility) {
    emit('update:modelValue', drawerVisibility);
  },
});

async function onSubmit() {
  let hasErrorOccurred = false;
  // Handle deleted fields
  if (deletedFields.length) {
    try {
      // Show confirmation modal before deleting attributes
      await ConfirmDialog({
        type: 'error',
        title: 'Deleting global attributes in use',
        message:
          'Deleting global attributes will also discard any associated values. \n Are you sure you want to proceed?',
        confirmButtonText: 'Confirm update',
      });

      trackEvent({
        key: FeatureEventKey.DELETE_GLOBAL_ATTRIBUTE,
        type: EventType.FEATURE,
        properties: {
          data: deletedFields,
        },
      });

      const ids = deletedFields.map(
        (deletedField) => deletedField.id,
      );

      store
        .dispatch('member/doDestroyCustomAttributes', ids)
        .catch(() => {
          hasErrorOccurred = true;
        });
    } catch (e) {
      return;
    }
  }

  // Handle added fields
  if (addedFields.length) {
    trackEvent({
      key: FeatureEventKey.ADD_GLOBAL_ATTRIBUTE,
      type: EventType.FEATURE,
      properties: {
        data: addedFields,
      },
    });

    addedFields.forEach(async ({ type, label }) => {
      try {
        await store.dispatch(
          'member/doCreateCustomAttributes',
          {
            type,
            label,
          },
        );
      } catch (e) {
        hasErrorOccurred = true;
      }
    });
  }

  // Handle edited fields
  if (editedFields.length) {
    trackEvent({
      key: FeatureEventKey.EDIT_GLOBAL_ATTRIBUTE,
      type: EventType.FEATURE,
      properties: {
        data: editedFields,
      },
    });

    editedFields.forEach(async ({ id, label }) => {
      try {
        await store.dispatch(
          'member/doUpdateCustomAttributes',
          {
            id,
            data: {
              label,
            },
          },
        );
      } catch (e) {
        hasErrorOccurred = true;
      }
    });
  }

  if (hasErrorOccurred) {
    ToastStore.error('Ops, an error occurred');
  } else {
    ToastStore.success(
      'Custom Attributes successfully updated',
    );
    isDrawerOpen.value = false;
  }
}

function onInputChange(newValue, attribute) {
  // Logic for edited attributes
  if (
    model.value[attribute.name].canDelete
    && newValue
      !== initialModel.value[attribute.name]?.label
    && !editedFields.some(
      (field) => field.name === attribute.name,
    )
  ) {
    editedFields.push(model.value[attribute.name]);
  } else if (
    model.value[attribute.name].canDelete
    && newValue === initialModel.value[attribute.name]?.label
  ) {
    const id = editedFields.findIndex(
      (field) => field.name === attribute.name,
    );
    editedFields.splice(id, 1);
  }
}

function onReset() {
  addedFields.splice(0);
  editedFields.splice(0);
  deletedFields.splice(0);

  model.value = cloneDeep(initialModel.value);
}

function addAttribute() {
  const newAttribute = {
    name: Date.now(),
    type: 'string',
    label: null,
  };

  addedFields.push(newAttribute);
  model.value[newAttribute.name] = newAttribute;
}

function deleteAttribute(key) {
  if (
    model.value[key].canDelete
    && !deletedFields.some((field) => field.name === key)
  ) {
    deletedFields.push(model.value[key]);
  } else {
    const id = addedFields.findIndex((a) => a.name === key);

    if (id !== -1) {
      addedFields.splice(id, 1);
    }
  }

  delete model.value[key];
}

watch(
  () => customAttributes.value,
  (updatedStore) => {
    onReset();
    initialModel.value = cloneDeep(
      parseCustomAttributes(updatedStore),
    );

    model.value = cloneDeep(
      parseCustomAttributes(updatedStore),
    );
  },
);
</script>

<style lang="scss">
.member-attributes-drawer {
  & .el-drawer__header {
    @apply p-6;
  }

  & .el-drawer__body {
    @apply px-6 py-0;
  }

  & .el-drawer__footer {
    @apply p-6 border-t border-gray-200;
  }

  & .attribute-type {
    width: 100px;
  }

  & .el-form-item,
  .el-form .el-form-item__content {
    @apply mb-0;
  }
}
</style>
