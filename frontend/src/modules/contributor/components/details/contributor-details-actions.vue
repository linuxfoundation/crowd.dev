<template>
  <div class="flex">
    <lf-button-group>
      <!-- Merge suggestions -->
      <lf-button
        type="secondary"
        @click="setReportDataModal({
          contributor: props.contributor,
        })"
      >
        <lf-icon name="message-exclamation fa-sharp" class="text-red-500" /> Report data issue
      </lf-button>
      <lf-button
        v-if="mergeSuggestionsCount > 0 && hasPermission(LfPermission.mergeMembers)"
        type="secondary"
        @click="isMergeSuggestionsDialogOpen = true"
      >
        <div class="bg-primary-500 text-white text-medium leading-5 px-1.5 rounded font-semibold">
          {{ mergeSuggestionsCount }}
        </div>
        {{ pluralize('Merge suggestion', mergeSuggestionsCount) }}
      </lf-button>

      <!-- Merge -->
      <lf-button
        v-else-if="hasPermission(LfPermission.mergeMembers)"
        type="secondary"
        @click="isMergeDialogOpen = props.contributor"
      >
        <lf-icon name="merge" />
        Merge profile
      </lf-button>

      <!-- Actions -->
      <lf-dropdown
        v-if="hasPermission(LfPermission.memberEdit) || hasPermission(LfPermission.memberDestroy)"
        class="z-20"
        placement="bottom-end"
      >
        <template #trigger>
          <lf-button
            type="secondary"
            :icon-only="true"
            :class="hasPermission(LfPermission.mergeMembers) ? '!rounded-l-none -ml-px' : ''"
          >
            <lf-icon name="ellipsis" />
          </lf-button>
        </template>

        <lf-contributor-dropdown
          :contributor="props.contributor"
          @reload="emit('reload')"
          @find-github="isFindGithubDrawerOpen = props.contributor"
          @unmerge="isUnmergeDialogOpen = props.contributor"
        />
      </lf-dropdown>
    </lf-button-group>
  </div>
  <app-member-merge-suggestions-dialog
    v-if="isMergeSuggestionsDialogOpen"
    v-model="isMergeSuggestionsDialogOpen"
    :query="{
      filter: {
        memberId: props.contributor?.id,
      },
      segments: [],
    }"
  />
  <app-member-merge-dialog
    v-if="isMergeDialogOpen"
    v-model="isMergeDialogOpen"
    :to-merge="contributorToMerge"
  />
  <app-member-find-github-drawer
    v-if="isFindGithubDrawerOpen"
    v-model="isFindGithubDrawerOpen"
  />
  <app-member-unmerge-dialog
    v-if="isUnmergeDialogOpen !== null"
    v-model="isUnmergeDialogOpen"
  />
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfButtonGroup from '@/ui-kit/button/ButtonGroup.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import { onMounted, ref, watch } from 'vue';
import AppMemberMergeSuggestionsDialog from '@/modules/member/components/member-merge-suggestions-dialog.vue';
import AppMemberMergeDialog from '@/modules/member/components/member-merge-dialog.vue';
import AppMemberFindGithubDrawer from '@/modules/member/components/member-find-github-drawer.vue';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { useMemberStore } from '@/modules/member/store/pinia';
import { storeToRefs } from 'pinia';
import { MemberService } from '@/modules/member/member-service';
import pluralize from 'pluralize';
import LfContributorDropdown from '@/modules/contributor/components/shared/contributor-dropdown.vue';
import { ContributorApiService } from '@/modules/contributor/services/contributor.api.service';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import { useSharedStore } from '@/shared/pinia/shared.store';
import AppMemberUnmergeDialog from '@/modules/member/components/member-unmerge-dialog.vue';

const props = defineProps<{
  contributor: Contributor,
}>();

const emit = defineEmits<{(e: 'reload'): any}>();

const { hasPermission } = usePermissions();

const { setReportDataModal } = useSharedStore();

const isMergeSuggestionsDialogOpen = ref<boolean>(false);
const isMergeDialogOpen = ref<Contributor | null>(null);
const isUnmergeDialogOpen = ref<Contributor | null>(null);
const isFindGithubDrawerOpen = ref<Contributor | null>(null);
const mergeSuggestionsCount = ref<number>(0);

const memberStore = useMemberStore();
const { toMergeMember } = storeToRefs(memberStore);
const contributorToMerge = ref<Contributor | null>(null);

watch(toMergeMember, (updatedValue) => {
  if (updatedValue) {
    MemberService.find(updatedValue.id, updatedValue.segmentId).then((response) => {
      isMergeDialogOpen.value = props.contributor;
      contributorToMerge.value = response;

      memberStore.removeToMergeMember();
    });
  }
}, {
  deep: true,
});

const fetchMergeSuggestions = () => {
  ContributorApiService.mergeSuggestions(0, 0, {
    filter: {
      memberId: props.contributor.id,
    },
    detail: false,
    countOnly: true,
  }, [])
    .then(({ count }) => {
      mergeSuggestionsCount.value = +count;
    });
};

onMounted(() => {
  fetchMergeSuggestions();
});
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsActions',
};
</script>
