<template>
  <div class="flex items-center gap-1">
    <!-- Singular -->
    <el-tooltip
      v-for="([orgId, aff]) of Object.entries(props.project.affiliations).slice(0, 1)"
      :key="orgId"
      placement="top"
    >
      <template #content>
        <p class="text-left">
          <span class="font-semibold">Affiliation period: </span>
          <span v-html="getAffilationPeriodText(aff)" />
        </p>
      </template>
      <router-link
        :to="{
          name: 'organizationView',
          params: { id: orgId },
          query: {
            projectGroup: selectedProjectGroup?.id,
            segmentId: aff[0].segmentId,
          },
        }"
      >
        <lf-badge
          type="secondary"
          class="!flex items-center hover:text-primary-500"
        >
          <lf-avatar
            :src="aff[0].organizationLogo"
            :name="aff[0].organizationName"
            :size="14"
            class="!rounded border-2 border-gray-200 mr-1 mt-px"
          >
            <template #placeholder>
              <div class="w-full h-full bg-gray-50 flex items-center justify-center">
                <lf-icon name="house-building" :size="12" class="text-gray-400" />
              </div>
            </template>
          </lf-avatar>
          <div class="truncate" style="max-width: 15ch">
            {{ aff[0].organizationName }}
          </div>
        </lf-badge>
      </router-link>
    </el-tooltip>

    <!-- The rest -->
    <el-popover placement="top" width="20rem">
      <template #reference>
        <lf-badge
          v-if="Object.keys(props.project.affiliations).length > 1"
          type="secondary"
        >
          +{{ Object.keys(props.project.affiliations).length - 1 }}
        </lf-badge>
      </template>
      <div class="p-1">
        <p class="text-small font-semibold text-gray-400 pb-4">
          Affiliation
        </p>
        <div class="flex flex-col gap-4">
          <article
            v-for="([orgId, aff]) of Object.entries(props.project.affiliations)"
            :key="orgId"
            class="flex justify-between"
          >
            <router-link
              :to="{
                name: 'organizationView',
                params: { id: orgId },
                query: {
                  projectGroup: selectedProjectGroup?.id,
                  segmentId: aff[0].segmentId,
                },
              }"
              class="cursor-pointer text-small leading-5 underline decoration-dashed text-gray-500
             decoration-gray-500 underline-offset-4 hover:decoration-gray-900 hover:!text-black flex items-center"
            >
              <lf-avatar
                :src="aff[0].organizationLogo"
                :name="aff[0].organizationName"
                :size="16"
                class="!rounded border-gray-200 mr-1 mt-px border-2"
              >
                <template #placeholder>
                  <div class="w-full h-full bg-gray-50 flex items-center justify-center">
                    <lf-icon name="house-building" :size="12" class="text-gray-400" />
                  </div>
                </template>
              </lf-avatar>
              <div style="max-width: 18ch" class="truncate">
                {{ aff[0].organizationName }}
              </div>
            </router-link>
            <p class="text-small text-gray-400">
              <span v-html="getAffilationPeriodText(aff)" />
            </p>
          </article>
        </div>
      </div>
    </el-popover>
  </div>
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { ContributorAffiliation } from '@/modules/contributor/types/Contributor';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  project: any,
}>();

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const getAffilationPeriodText = (affilations: ContributorAffiliation[]) => affilations.map(({ dateStart, dateEnd }) => {
  const start = dateStart
    ? dateHelper(dateStart).utc().format('MMM YYYY')
    : 'Unknown';
  const endDefault = dateStart ? 'Present' : 'Unknown';
  const end = dateEnd
    ? dateHelper(dateEnd).utc().format('MMM YYYY')
    : endDefault;
  if (start === end) {
    return start;
  }
  return `${start} → ${end}`;
}).join(' <br> ');

</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsProjectsAffiliation',
};
</script>
