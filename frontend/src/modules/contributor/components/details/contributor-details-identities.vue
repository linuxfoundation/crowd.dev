<template>
  <section v-bind="$attrs">
    <div class="flex justify-between items-center pb-5">
      <h6 class="text-h6">
        Identities
      </h6>
      <lf-contributor-details-identity-add-dropdown
        v-if="hasPermission(LfPermission.memberEdit)"
        placement="bottom-end"
        @add="
          addIdentity = true;
          addIdentityTemplate = $event;
        "
      >
        <lf-tooltip content="Add identity">
          <lf-button
            type="secondary"
            size="small"
            icon-only
            class="my-1"
          >
            <lf-icon name="plus" />
          </lf-button>
        </lf-tooltip>
      </lf-contributor-details-identity-add-dropdown>
    </div>

    <div class="flex flex-col gap-3">
      <lf-contributor-details-identity-item
        v-for="identity of identityList.slice(
          0,
          showMore ? identityList.length : 10,
        )"
        :key="`${identity.platform}-${identity.value}`"
        :identity="identity"
        :contributor="props.contributor"
        class="min-h-7"
        @edit="editIdentity = identity"
        @unmerge="unmerge(identity.id)"
      />

      <div
        v-if="identityList.length === 0"
        class="pt-2 flex flex-col items-center"
      >
        <lf-icon name="fingerprint" :size="40" class="text-gray-300" />
        <p class="text-center pt-3 text-medium text-gray-400">
          No identities
        </p>
      </div>
    </div>

    <lf-button
      v-if="identityList.length > 10"
      type="primary-link"
      size="medium"
      class="mt-6"
      @click="showMore = !showMore"
    >
      Show {{ showMore ? "less" : "more" }}
    </lf-button>
  </section>
  <lf-contributor-identity-add
    v-if="addIdentity && addIdentityTemplate !== null"
    v-model="addIdentity"
    :identities="[addIdentityTemplate]"
    :contributor="props.contributor"
  />
  <lf-contributor-identity-edit
    v-if="editIdentity !== null"
    v-model="editIdentity"
    :contributor="props.contributor"
  />
  <app-member-unmerge-dialog
    v-if="isUnmergeDialogOpen"
    v-model="isUnmergeDialogOpen"
    :selected-identity="selectedIdentity"
  />
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import type {
  Contributor,
  ContributorIdentity,
} from '@/modules/contributor/types/Contributor';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';
import { computed, ref } from 'vue';
import AppMemberUnmergeDialog from '@/modules/member/components/member-unmerge-dialog.vue';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import LfContributorDetailsIdentityItem from '@/modules/contributor/components/details/identity/contributor-details-identity-item.vue';
import LfContributorDetailsIdentityAddDropdown from '@/modules/contributor/components/details/identity/contributor-details-identity-add-dropdown.vue';
import LfContributorIdentityEdit from '@/modules/contributor/components/edit/identity/contributor-identity-edit.vue';
import LfContributorIdentityAdd from '@/modules/contributor/components/edit/identity/contributor-identity-add.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  contributor: Contributor;
}>();

const { hasPermission } = usePermissions();

const { identities, emails } = useContributorHelpers();

const identityList = computed(() => {
  const caseSensitivePlatforms = ['git'];
  const seen = new Set<string>();
  const result: ContributorIdentity[] = [];
  const items = identities(props.contributor) as ContributorIdentity[];
  items.forEach((identity) => {
    const key = `${identity.value.toLowerCase()}-${identity.platform}`;
    if (
      identity.verified
      && !caseSensitivePlatforms.includes(identity.platform)
    ) {
      if (!seen.has(key)) {
        seen.add(key);
        result.push(identity);
      } else {
        const existingIdentity = result.find(
          (item) => item.value.toLowerCase() === identity.value.toLowerCase()
            && item.platform === identity.platform,
        );
        if (existingIdentity && existingIdentity.duplicatedIdentities?.length) {
          existingIdentity.duplicatedIdentities.push(identity);
        } else if (existingIdentity) {
          existingIdentity.duplicatedIdentities = [identity];
        }
      }
    } else {
      result.push(identity); // always include unVerified identities
    }
  });

  return [...result, ...emails(props.contributor)];
});

const showMore = ref<boolean>(false);
const isUnmergeDialogOpen = ref<Contributor | null>(null);
const selectedIdentity = ref<string | null>(null);

const addIdentity = ref<boolean>(false);
const addIdentityTemplate = ref<Partial<ContributorIdentity> | null>(null);
const editIdentity = ref<Partial<ContributorIdentity> | null>(null);

const unmerge = (identityId: string) => {
  if (identityId) {
    selectedIdentity.value = identityId;
  }
  isUnmergeDialogOpen.value = props.contributor as any;
};
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsIdentities',
};
</script>
