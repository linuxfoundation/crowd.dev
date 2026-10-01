<template>
  <!-- This drawer is too customized to use the app-drawer generic component -->
  <el-drawer
    v-model="drawerModel"
    size="35%"
    :show-close="false"
    destroy-on-close
    :close-on-click-modal="false"
    :custom-class="
      isEditingAttributes
        ? 'edit-attribute-drawer'
        : 'manage-attribute-drawer'
    "
  >
    <template #header="{ close, titleId, titleClass }">
      <div class="flex grow justify-between items-center">
        <div>
          <lf-button
            v-if="isEditingAttributes"
            type="secondary-link"
            size="medium"
            class="mb-2"
            @click="onCloseManageAttributes"
          >
            <lf-icon name="chevron-left" :size="16" />
            <span>Edit attributes</span>
          </lf-button>
          <h5
            :id="titleId"
            class="text-black"
            :class="titleClass"
          >
            {{
              !isEditingAttributes
                ? 'Edit attributes'
                : 'Manage global attributes'
            }}
          </h5>
          <lf-button
            v-if="!isEditingAttributes"
            type="primary-link"
            size="small"
            @click="onOpenManageAttributes"
          >
            Manage global attributes
          </lf-button>
        </div>

        <lf-button
          type="primary-link"
          size="tiny"
          class="w-8 !h-8"
          icon-only
          @click="close"
        >
          <lf-icon name="xmark" :size="20" class="text-gray-400" />
        </lf-button>
      </div>
    </template>
    <template #default>
      <div v-if="!isEditingAttributes">
        <app-member-form-attributes
          v-model="memberModel"
          :show-header="false"
          :attributes="computedAttributes"
          :record="member"
        />
      </div>
      <app-member-form-global-attributes
        v-else
        v-model="isEditingAttributes"
      />
    </template>
    <template v-if="!isEditingAttributes" #footer>
      <div
        class="flex w-full justify-end"
        :class="
          hasFormChanged ? 'justify-between' : 'justify-end'
        "
      >
        <lf-button
          v-if="hasFormChanged"
          type="primary-link"
          @click="handleReset"
        >
          <lf-icon name="arrow-turn-left" :size="16" />
          <span>Reset changes</span>
        </lf-button>
        <div class="flex gap-4">
          <lf-button
            :disabled="loading"
            type="secondary-gray"
            size="medium"
            @click="handleCancel"
          >
            Cancel
          </lf-button>
          <lf-button
            :disabled="!hasFormChanged || loading"
            type="primary"
            size="medium"
            :loading="loading"
            @click="handleSubmit"
          >
            Update
          </lf-button>
        </div>
      </div>
    </template>
  </el-drawer>
</template>

<script setup>
import {
  ref,
  computed,
} from 'vue';
import isEqual from 'lodash/isEqual';
import cloneDeep from 'lodash/cloneDeep';

import { ToastStore } from '@/shared/message/notification';
import getAttributesModel from '@/shared/attributes/get-attributes-model';
import getParsedAttributes from '@/shared/attributes/get-parsed-attributes';
import { useMemberStore } from '@/modules/member/store/pinia';
import { storeToRefs } from 'pinia';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { useRoute } from 'vue-router';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import AppMemberFormGlobalAttributes from './form/member-form-global-attributes.vue';
import AppMemberFormAttributes from './form/member-form-attributes.vue';

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false,
  },
  member: {
    type: Object,
    default: () => {},
  },
});
const emit = defineEmits(['update:modelValue']);

const route = useRoute();
const { trackEvent } = useProductTracking();

const memberStore = useMemberStore();
const { customAttributes } = storeToRefs(memberStore);

const { updateContributorAttributes } = useContributorStore();

const loading = ref(false);
const isEditingAttributes = ref(false);

const computedAttributes = computed(() => Object.values(customAttributes.value));
const drawerModel = computed({
  get() {
    return props.modelValue;
  },
  set(value) {
    emit('update:modelValue', value);
  },
});
const initialModel = computed(() => {
  const attributes = getAttributesModel(props.member);

  return {
    ...props.member,
    ...(Object.keys(attributes).length && attributes),
  };
});
// eslint-disable-next-line vue/no-ref-object-reactivity-loss -- intentional initial snapshot
const memberModel = ref(cloneDeep(initialModel.value));

const hasFormChanged = computed(() => !isEqual(
  cloneDeep(initialModel.value),
  memberModel.value,
));

const handleReset = () => {
  memberModel.value = cloneDeep(initialModel.value);
};

const handleCancel = () => {
  emit('update:modelValue', false);
};

const handleSubmit = async () => {
  loading.value = true;

  trackEvent({
    key: FeatureEventKey.EDIT_MEMBER_ATTRIBUTES,
    type: EventType.FEATURE,
    properties: {
      path: route.path,
    },
  });

  const formattedAttributes = getParsedAttributes(
    computedAttributes.value,
    memberModel.value,
  );

  Object.keys(formattedAttributes).forEach((key) => {
    if (!formattedAttributes[key]) {
      delete formattedAttributes[key];
    }
  });

  await updateContributorAttributes(props.member.id, {
    ...props.member.attributes,
    ...formattedAttributes,
  });
  ToastStore.success('Member attributes updated successfully');
  emit('update:modelValue', false);
};

const onOpenManageAttributes = () => {
  isEditingAttributes.value = true;
};

const onCloseManageAttributes = () => {
  isEditingAttributes.value = false;
};
</script>

<script>
export default {
  name: 'AppMemberManageAttributesDrawer',
};
</script>

<style lang="scss">
.manage-attribute-drawer {
  .el-form-item,
  .el-form-item__content {
    @apply mb-0;
  }
}

.edit-attribute-drawer {
  & .el-drawer__header {
    @apply p-6;
  }

  & .el-drawer__body {
    @apply p-0;
  }

  & .el-drawer__footer {
    @apply w-full p-6 border-t border-gray-200;
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
