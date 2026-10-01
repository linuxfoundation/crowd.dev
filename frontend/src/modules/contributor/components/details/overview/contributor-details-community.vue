<template>
  <section>
    <lf-card class="!bg-gradient-to-b from-primary-25 to-white px-5 pt-5 pb-6">
      <h6 class="text-h6 mb-4">
        Community snapshot
      </h6>
      <div class="flex flex-wrap gap-y-4 -mx-4 overflow-hidden">
        <article class="px-4 h-full flex-grow w-1/2 xl:w-auto border-r border-gray-200 -mr-px">
          <div class="flex items-center gap-1 mb-2">
            <p class="text-tiny text-secondary-300">
              Role
            </p>
            <el-popover placement="top" width="20rem">
              <template #reference>
                <lf-icon name="circle-question" :size="14" class="text-secondary-200 font-normal" />
              </template>
              <div class="p-1">
                <p class="text-small font-semibold mb-2 text-black">
                  Maintainer
                </p>
                <p class="text-small text-gray-500 break-normal mb-5 text-left">
                  Individual responsible for overseeing and managing code repositories by
                  reviewing and merging pull requests, addressing issues, ensuring code quality, and guiding contributors.
                </p>
                <p class="text-small font-semibold mb-2 text-black">
                  Contributor
                </p>
                <p class="text-small text-gray-500 break-normal text-left">
                  Someone who has contributed to a project by making changes or additions to its code.
                  Contributions require that code was successfully merged into a repository.
                </p>
              </div>
            </el-popover>
          </div>
          <lf-contributor-details-maintainer :contributor="props.contributor" />
        </article>
        <article class="px-4 h-full flex-grow w-1/2 xl:w-auto border-r border-gray-200 -mr-px">
          <div class="flex items-center gap-1 mb-2">
            <p class="text-tiny text-secondary-300">
              Engagement level
            </p>
            <el-tooltip placement="top">
              <template #content>
                Calculated based on the recency and importance<br> of a person's
                activities in comparison<br> to the community.
              </template>
              <lf-icon name="circle-question" :size="14" class="text-secondary-200" />
            </el-tooltip>
          </div>
          <lf-contributor-engagement-level :contributor="props.contributor" />
        </article>
        <article class="px-4 h-full flex-grow w-1/2 xl:w-auto border-r border-gray-200 -mr-px">
          <p class="text-tiny text-secondary-300 mb-2">
            Avg. sentiment
          </p>
          <lf-contributor-sentiment :contributor="props.contributor" />
        </article>
        <article class="px-4 h-full flex-grow w-1/2 xl:w-auto border-r border-gray-200 -mr-px">
          <p class="text-tiny text-secondary-300 mb-2">
            # of activities
          </p>
          <lf-loading
            v-if="props.contributor.activitySycning?.state === MergeActionState.IN_PROGRESS"
            :count="1"
            height="1rem"
            width="4rem"
            class="rounded"
          />
          <p v-else class="text-small text-gray-600">
            {{ props.contributor.activityCount && formatNumber(props.contributor.activityCount) || '-' }}
          </p>
        </article>
        <article class="px-4 h-full flex-grow w-1/2 xl:w-auto border-r border-gray-200 -mr-px">
          <p class="text-tiny text-secondary-300 mb-2">
            Joined date
          </p>
          <p class="text-small text-gray-600">
            {{ dateHelper(props.contributor.joinedAt).isAfter(dateHelper('1970-01-01', 'year'))
              ? dateHelper(props.contributor.joinedAt).format('MMM DD, YYYY') : '-' }}
          </p>
        </article>
      </div>
    </lf-card>
  </section>
</template>

<script setup lang="ts">
import LfCard from '@/ui-kit/card/Card.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { formatNumber } from '@/utils/number';
import LfContributorEngagementLevel from '@/modules/contributor/components/shared/contributor-engagement-level.vue';
import LfContributorSentiment from '@/modules/contributor/components/shared/contributor-sentiment.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import LfLoading from '@/ui-kit/loading/Loading.vue';
import { MergeActionState } from '@/shared/modules/merge/types/MemberActions';
import LfContributorDetailsMaintainer
  from '@/modules/contributor/components/details/overview/contributor-details-maintainer.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  contributor: Contributor,
}>();
</script>
<script lang="ts">
export default {
  name: 'LfContributorDetailsCommunity',
};
</script>
