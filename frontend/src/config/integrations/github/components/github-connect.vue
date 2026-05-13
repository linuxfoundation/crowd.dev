<template>
  <div>
    <lfx-dropdown
      placement="bottom-end"
      width="20rem"
      :visibility="isDropdownVisible"
      @update:visibility="isDropdownVisible = $event"
    >
      <template #trigger>
        <lf-button type="outline" :class="{ 'is-active': isDropdownVisible }">
          <lf-icon name="link-simple" />
          <slot>Connect</slot>
        </lf-button>
      </template>

      <lfx-dropdown-item @click="isV2SettingsDrawerOpen = true">
        <div class="flex items-start gap-2">
          <lf-github-version-tag version="v2" />
          <div>
            <div class="text-sm text-neutral-900">
              GitHub - New integration
            </div>
            <p class="text-xs text-neutral-500">
              Sync repositories from multiple GitHub organizations. Doesn’t require organization admin permissions.
            </p>
          </div>
        </div>
      </lfx-dropdown-item>
      <lfx-dropdown-item @click="isV1SettingsDrawerOpen = true">
        <div class="flex items-start gap-2">
          <lf-github-version-tag version="v1" />
          <div>
            <div class="text-sm text-neutral-900">
              GitHub - Old integration
            </div>
            <p class="text-xs text-neutral-500">
              Sync repositories from a GitHub organization. Requires organization admin permissions.
            </p>
          </div>
        </div>
      </lfx-dropdown-item>
    </lfx-dropdown>
  </div>

  <!-- V2 Settings Drawer -->
  <lf-github-nango-settings-drawer
    v-if="isV2SettingsDrawerOpen"
    v-model="isV2SettingsDrawerOpen"
    :integration="props.integration"
    :segment-id="props.segmentId"
    :grandparent-id="props.grandparentId"
  />

  <!-- V1 Settings Drawer -->
  <lf-github-connect-modal
    v-if="isV1SettingsDrawerOpen"
    v-model="isV1SettingsDrawerOpen"
  />
  <lf-github-connect-finishing-modal v-model="isFinishingModalOpen" />
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfxDropdown from '@/ui-kit/lfx/dropdown/dropdown.vue';
import LfxDropdownItem from '@/ui-kit/lfx/dropdown/dropdown-item.vue';
import LfGithubNangoSettingsDrawer from '@/config/integrations/github-nango/components/settings/github-settings-drawer.vue';
import LfGithubConnectModal from '@/config/integrations/github/components/connect/github-connect-modal.vue';
import LfGithubConnectFinishingModal from '@/config/integrations/github/components/connect/github-connect-finishing-modal.vue';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import { useRoute } from 'vue-router';
import LfGithubVersionTag from '@/config/integrations/github/components/github-version-tag.vue';

const route = useRoute();
const props = defineProps<{
  integration: any,
  segmentId: string | null;
  grandparentId: string | null;
}>();

const isV2SettingsDrawerOpen = ref(false);
const isV1SettingsDrawerOpen = ref(false);
const isDropdownVisible = ref(false);
const isFinishingModalOpen = ref(false);

const { doGithubConnect } = mapActions('integration');

const finalizeGithubConnect = () => {
  const {
    code, source, state,
  } = route.query;
  const setupAction = route.query.setup_action;
  const installId = route.query.installation_id;

  if (code && !source && state !== 'noconnect') {
    isFinishingModalOpen.value = true;
    doGithubConnect({
      code,
      installId,
      setupAction,
    }).then(() => {
      isFinishingModalOpen.value = false;
    });
  }
};

onMounted(() => {
  finalizeGithubConnect();
});
</script>

<script lang="ts">
export default {
  name: 'LfGithubConnect',
};
</script>
