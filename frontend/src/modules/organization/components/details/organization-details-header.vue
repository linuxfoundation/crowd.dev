<template>
  <div class="flex items-center flex-grow">
    <div @mouseover.stop @mouseout.stop>
      <lf-organization-details-header-logo :organization="props.organization" />
    </div>
    <div class="pl-3 w-full">
      <lf-organization-edit-name :organization="props.organization" />
      <div class="flex items-center gap-1.5">
        <lf-organization-membership :organization="props.organization" />
        <p v-if="!!props.organization.lfxMembership" class="text-small text-gray-400">
          •
        </p>
        <template v-if="props.organization.isTeamOrganization">
          <lf-badge size="small">
            Team
          </lf-badge>
          <p v-if="props.organization.isTeamOrganization" class="text-small text-gray-400">
            •
          </p>
        </template>
        <a
          v-if="!!websiteCom"
          :href="withHttp(websiteCom.value)"
          class="flex items-center gap-1"
          target="_blank"
          rel="noopener noreferrer"
        >
          <lf-icon name="link" :size="16" class="text-gray-400" />
          <div class="text-gray-500 text-small truncate hover:text-primary-500" style="max-width: 20ch">
            {{ websiteCom.value }}
          </div>
        </a>
        <p v-if="hasHeaderIdentities && !!websiteCom" class="text-small text-gray-400">
          •
        </p>
        <lf-organization-details-header-profiles :organization="props.organization" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import LfBadge from '@/ui-kit/badge/Badge.vue';
import type { Organization } from '@/modules/organization/types/Organization';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import LfOrganizationMembership from '@/modules/organization/components/shared/organization-membership.vue';
import { withHttp } from '@/utils/string';
import LfOrganizationDetailsHeaderProfiles
  from '@/modules/organization/components/details/header/organization-details-header-profiles.vue';
import { computed } from 'vue';
import {
  organizationDetailsHeaderProfilePlatforms,
} from '@/modules/organization/config/details-header-profile-platforms';
import LfOrganizationEditName from '@/modules/organization/components/edit/organization-edit-name.vue';
import LfOrganizationDetailsHeaderLogo
  from '@/modules/organization/components/details/header/organization-details-header-logo.vue';

const props = defineProps<{
  organization: Organization,
}>();

const { website } = useOrganizationHelpers();

const websiteCom = computed(() => website(props.organization));

const hasHeaderIdentities = computed(() => props.organization.identities.some((i) => organizationDetailsHeaderProfilePlatforms.includes(i.platform)));
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsHeader',
};
</script>
