<template>
  <section v-bind="$attrs">
    <div class="flex justify-between items-center pb-6">
      <div class="flex items-center">
        <h6 class="text-h6">
          Work history
        </h6>
        <div class="pl-1">
          <lf-tooltip placement="bottom">
            <template #content>
              Work experiences are mostly obtained<br> via enrichment but can also be added <br>manually.
            </template>
            <lf-icon name="circle-question" :size="16" class="text-gray-400" />
          </lf-tooltip>
        </div>
      </div>
      <lf-tooltip
        v-if="hasPermission(LfPermission.memberEdit)"
        content="Add work experience"
        content-class="-ml-5"
      >
        <lf-button
          type="secondary"
          size="small"
          :icon-only="true"
          @click="isEditModalOpen = true; editOrganization = null"
        >
          <lf-icon name="plus" />
        </lf-button>
      </lf-tooltip>
    </div>

    <div class="flex flex-col">
      <lf-timeline v-for="group in shownGroups" :key="group.id" width="1.5rem">
        <lf-timeline-item v-for="(item, ii) in group.items" :key="item.id">
          <template v-if="ii === 0 || item.memberOrganizations.affiliationOverride.isPrimaryWorkExperience" #dot>
            <div class="relative flex justify-center">
              <lf-avatar
                v-if="ii === 0"
                :src="item.logo"
                :name="item.displayName"
                :size="24"
                class="!rounded-md border border-gray-200 mb-1.5"
              />
              <div
                v-if="item.memberOrganizations.affiliationOverride.isPrimaryWorkExperience"
                :class="ii === 0 ? 'absolute -top-2 -right-2' : '-mt-0.5'"
              >
                <lf-tooltip content="Affiliated organization/job title" placement="top-start">
                  <div
                    class=" border-2 border-white bg-secondary-500 rounded-full w-4.5 h-4.5 flex items-center justify-center"
                  >
                    <lf-icon
                      name="link"
                      :size="8"
                      class="text-white font-bold"
                    />
                  </div>
                </lf-tooltip>
              </div>
            </div>
          </template>
          <div class="pb-4 pl-3 w-full">
            <lf-contributor-details-work-history-item
              :contributor="props.contributor"
              :organization="item"
              @edit="isEditModalOpen = true; editOrganization = item"
            >
              <template v-if="ii === 0" #above>
                <router-link
                  class="text-black w-fit"
                  :to="{
                    name: 'organizationView',
                    params: { id: item.id },
                    query: {
                      projectGroup: selectedProjectGroup?.id,
                      segmentId: props.contributor.segmentId,
                    },
                  }"
                >
                  <p class="font-semibold text-medium pt-0.5 pb-1.5">
                    {{ item.displayName }}
                  </p>
                </router-link>
              </template>
            </lf-contributor-details-work-history-item>
          </div>
        </lf-timeline-item>
      </lf-timeline>

      <div v-if="orgGrouped.length === 0" class="pt-2 flex flex-col items-center">
        <lf-icon name="clipboard-list fa-sharp" :size="40" class="text-gray-300" />
        <p class="text-center pt-3 text-medium text-gray-400">
          No work experiences
        </p>
      </div>
    </div>

    <lf-button
      v-if="orgGrouped.length > minimumShownGroups"
      type="primary-link"
      size="medium"
      class="mt-6"
      @click="showMore = !showMore"
    >
      Show {{ showMore ? 'less' : 'more' }}
    </lf-button>
  </section>

  <lf-contributor-edit-work-history
    v-if="isEditModalOpen"
    v-model="isEditModalOpen"
    :organization="editOrganization"
    :contributor="props.contributor"
  />
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import { computed, ref } from 'vue';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import LfContributorEditWorkHistory
  from '@/modules/contributor/components/edit/work-history/contributor-work-history-edit.vue';
import type { Organization } from '@/modules/organization/types/Organization';
import LfContributorDetailsWorkHistoryItem
  from '@/modules/contributor/components/details/work-history/contributor-details-work-history-item.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { TimelineGroup } from '@/ui-kit/timeline/types/TimelineTypes';
import { groupBy } from 'lodash';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfTimeline from '@/ui-kit/timeline/Timeline.vue';
import LfTimelineItem from '@/ui-kit/timeline/TimelineItem.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';

const props = defineProps<{
  contributor: Contributor,
}>();

const { hasPermission } = usePermissions();
const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const showMore = ref<boolean>(false);
const isEditModalOpen = ref<boolean>(false);
const editOrganization = ref<Organization | null>(null);

const orgGrouped = computed(() => {
  const grouped = groupBy(props.contributor.organizations, 'id');
  return Object.keys(grouped).map((id, index): TimelineGroup => ({
    id: index,
    label: grouped[id][0].displayName,
    labelLink: {
      name: 'organizationView',
      params: {
        id,
      },
      query: {
        projectGroup: selectedProjectGroup.value?.id,
      },
    },
    icon: grouped[id][0].logo,
    items: grouped[id],
  }));
});
const minimumShownGroups = 3;
const shownGroups = computed(() => orgGrouped.value.slice(0, showMore.value ? orgGrouped.value.length : minimumShownGroups));
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsWorkHistory',
};
</script>
