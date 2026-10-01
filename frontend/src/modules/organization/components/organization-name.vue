<template>
  <div class="flex items-center gap-3">
    <div>
      <div
        class="min-h-8 min-w-8 w-8 h-8 border border-gray-200 rounded-md flex items-center justify-center relative overflow-hidden"
        :class="{
          'bg-white': organization.logo,
          'bg-gray-50': !organization.logo,
        }"
      >
        <app-avatar-image
          :src="organization.logo"
          class="max-h-8"
        >
          <lf-icon name="house-building" :size="20" class="text-gray-300" />
        </app-avatar-image>
        <app-avatar-new-badge
          :entity="organization"
          is-small-avatar
          entity-name="organization"
        />
      </div>
    </div>
    <div class="inline-flex flex-wrap overflow-wrap items-center">
      <div class="flex items-center gap-1 mr-2">
        <el-tooltip
          :content="organization.displayName || organization.name"
          effect="dark"
          placement="top"
          :disabled="!showTooltip"
        >
          <div
            ref="nameRef"
            class="font-medium text-sm text-gray-900 line-clamp-2 w-auto"
            @mouseover="handleOnMouseOver"
            @mouseleave="handleOnMouseLeave"
          >
            {{ organization.displayName || organization.name }}
          </div>
        </el-tooltip>
        <lf-organization-lf-member-tag
          :organization="organization"
          only-show-icon
        />
      </div>
      <app-organization-badge
        class="mt-1"
        :organization="organization"
      />
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import AppOrganizationBadge from '@/modules/organization/components/organization-badge.vue';
import AppAvatarNewBadge from '@/shared/avatar/avatar-new-badge.vue';
import AppAvatarImage from '@/shared/avatar-image/avatar-image.vue';
import LfOrganizationLfMemberTag from '@/modules/organization/components/lf-member/organization-lf-member-tag.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

defineProps({
  organization: {
    type: Object,
    default: () => null,
  },
});

const nameRef = ref();
const showTooltip = ref(false);

const handleOnMouseOver = () => {
  if (!nameRef.value) {
    showTooltip.value = false;
  }

  showTooltip.value = nameRef.value.scrollWidth > nameRef.value.clientWidth;
};

const handleOnMouseLeave = () => {
  showTooltip.value = false;
};
</script>

<script>
export default {
  name: 'AppOrganizationName',
};
</script>
