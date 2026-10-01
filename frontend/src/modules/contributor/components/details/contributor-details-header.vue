<template>
  <div
    class="flex items-center flex-grow"
  >
    <div @mouseover.stop @mouseout.stop>
      <lf-contributor-details-header-profile-photo :contributor="props.contributor" />
    </div>

    <div class="pl-3 w-full">
      <lf-contributor-edit-name :contributor="props.contributor" />
      <div class="flex items-center gap-1.5">
        <lf-badge v-if="isTeamMember(props.contributor)" size="small">
          Team
        </lf-badge>
        <lf-badge v-if="isBot(props.contributor)" type="tertiary" size="small">
          Bot
        </lf-badge>
        <p v-if="isBot(props.contributor) || isTeamMember(props.contributor)" class="text-small text-gray-400">
          •
        </p>
        <div @mouseover.stop @mouseout.stop>
          <lf-contributor-work-position :contributor="props.contributor" />
        </div>
        <p v-if="hasHeaderIdentities && (jobTitle || organization)" class="text-small text-gray-400">
          •
        </p>
        <div @mouseover.stop @mouseout.stop>
          <lf-contributor-details-header-profiles
            :contributor="props.contributor"
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import LfBadge from '@/ui-kit/badge/Badge.vue';
import { computed } from 'vue';
import { contributorDetailsHeaderProfilePlatforms } from '@/modules/contributor/config/details-header-profile-platforms';
import LfContributorEditName from '@/modules/contributor/components/edit/contributor-edit-name.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';
import LfContributorWorkPosition from '@/modules/contributor/components/shared/contributor-work-position.vue';
import LfContributorDetailsHeaderProfiles
  from '@/modules/contributor/components/details/header/contributor-details-header-profiles.vue';
import LfContributorDetailsHeaderProfilePhoto
  from '@/modules/contributor/components/details/header/contributor-details-header-profile-photo.vue';

const props = defineProps<{
  contributor: Contributor,
}>();

const {
  isBot, isTeamMember, activeOrganization,
} = useContributorHelpers();

const hasHeaderIdentities = computed(
  () => (props.contributor.identities || []).some((i) => contributorDetailsHeaderProfilePlatforms.includes(i.platform)),
);

const organization = computed(() => activeOrganization(props.contributor));
const jobTitle = computed(() => props.contributor.attributes?.jobTitle?.default
    || organization.value?.memberOrganizations?.title);
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsHeader',
};
</script>
