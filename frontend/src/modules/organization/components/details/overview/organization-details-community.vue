<template>
  <section>
    <lf-card class="!bg-gradient-to-b from-primary-25 to-white px-5 pt-5 pb-6">
      <div class="flex justify-between pb-6">
        <h6 class="text-h6">
          Community snapshot
        </h6>
        <lf-organization-details-community-project-select
          v-if="!hasSegments"
          v-model:segment="selectedSegment"
          :organization="props.organization"
          @update:segment="loadData"
        />
      </div>
      <div class="flex flex-wrap gap-y-4 -mx-4">
        <article class="px-4 h-full w-1/2 xl:w-1/3">
          <p class="text-tiny text-secondary-300 mb-2">
            Community size
          </p>
          <lf-loading v-if="loadingMemberCount" :count="1" height="1rem" width="4rem" class="rounded" />
          <p v-else class="text-small text-gray-600">
            {{ pluralize('person', memberCount || 0, true) }}
          </p>
        </article>
        <article class="px-4 h-full w-1/2 xl:w-1/3 border-l border-gray-200">
          <p class="text-tiny text-secondary-300 mb-2">
            # of activities
          </p>
          <lf-loading
            v-if="props.organization.activitySycning?.state === MergeActionState.IN_PROGRESS"
            :count="1"
            height="1rem"
            width="4rem"
            class="rounded"
          />
          <p v-else class="text-small text-gray-600">
            {{ (activityCount && formatNumber(activityCount)) || '-' }}
          </p>
        </article>
        <article class="px-4 h-full w-1/2 xl:w-1/3 xl:border-l border-gray-200">
          <p class="text-tiny text-secondary-300 mb-2">
            Joined date
          </p>
          <p class="text-small text-gray-600">
            {{
              dateHelper(props.organization.joinedAt).isAfter(dateHelper(0))
                ? dateHelper(props.organization.joinedAt).format('MMM DD, YYYY')
                : '-'
            }}
          </p>
        </article>
      </div>
    </lf-card>
  </section>
</template>

<script setup lang="ts">
import LfCard from '@/ui-kit/card/Card.vue';
import { formatNumber } from '@/utils/number';
import type { Organization } from '@/modules/organization/types/Organization';
import pluralize from 'pluralize';
import {
  computed, onMounted, ref,
} from 'vue';
import { MemberService } from '@/modules/member/member-service';
import { MergeActionState } from '@/shared/modules/merge/types/MemberActions';
import LfLoading from '@/ui-kit/loading/Loading.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import LfOrganizationDetailsCommunityProjectSelect
  from '@/modules/organization/components/details/overview/community/organization-details-community-project-select.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  organization: Organization,
}>();

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const selectedSegment = ref<string>(selectedProjectGroup.value?.id || '');
const memberCount = ref<number>(0);
const loadingMemberCount = ref<boolean>(true);
const activityCount = computed<number>(() => props.organization.activityCount);

const doGetMembersCount = () => {
  loadingMemberCount.value = true;
  MemberService.listMembers(
    {
      limit: 1,
      offset: 0,
      filter: { organizations: { contains: [props.organization.id] } },
      segments: selectedSegment.value ? [selectedSegment.value] : props.organization.segments,
    },
    true,
  )
    .then(({ count }) => {
      memberCount.value = count;
    })
    .finally(() => {
      loadingMemberCount.value = false;
    });
};

const hasSegments = computed(() => selectedProjectGroup.value?.id);

const loadData = () => {
  doGetMembersCount();
};

onMounted(() => {
  loadData();
});
</script>
<script lang="ts">
export default {
  name: 'LfOrganizationDetailsCommunity',
};
</script>
