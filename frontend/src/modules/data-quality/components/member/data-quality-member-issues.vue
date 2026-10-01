<template>
  <div>
    <div v-if="loading && offset === 0" class="flex justify-center py-20">
      <lf-spinner />
    </div>
    <lf-scroll-body-controll v-else-if="members.length > 0" @bottom="loadMore()">
      <lf-data-quality-member-issues-item
        v-for="(member) of members"
        :key="member.id"
        :member="member"
        :type="props.type"
      >
        <template #action>
          <router-link
            :to="{
              name: 'memberView',
              params: { id: member.id },
              query: { projectGroup: selectedProjectGroup?.id },
            }"
            target="_blank"
            @click="trackReviewProfile(member.id)"
          >
            <lf-button type="secondary" size="small">
              <lf-icon name="arrow-up-right-from-square" />Review profile
            </lf-button>
          </router-link>
        </template>
      </lf-data-quality-member-issues-item>
      <div v-if="members.length < total" class="pt-4">
        <lf-button
          type="primary-ghost"
          loading-text="Loading issues..."
          :loading="loading"
          @click="loadMore()"
        >
          Load more
        </lf-button>
      </div>
    </lf-scroll-body-controll>
    <div v-else class="flex flex-col items-center pt-16">
      <lf-icon name="shuffle" :size="160" class="text-gray-200 flex items-center mb-8" />
      <h5 class="text-center text-lg font-semibold mb-4">
        No member issues found
      </h5>
      <p class="text-sm text-center text-gray-600 leading-5">
        We couldn't find any issues with members
      </p>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { onMounted, ref, watch } from 'vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import type { DataIssueType } from '@/modules/data-quality/types/DataIssueType';
import { DataQualityApiService } from '@/modules/data-quality/services/data-quality.api.service';
import LfDataQualityMemberIssuesItem
  from '@/modules/data-quality/components/member/data-quality-member-issues-item.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import LfScrollBodyControll from '@/ui-kit/scrollcontroll/ScrollBodyControll.vue';

const props = defineProps<{
  type: DataIssueType,
  projectGroup: string,
}>();

const loading = ref(true);
const limit = ref(20);
const offset = ref(0);
const total = ref(0);
const members = ref<any[]>([]);

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
const { trackEvent } = useProductTracking();

const loadDataIssues = () => {
  loading.value = true;
  if (offset.value === 0) {
    members.value = [];
  }
  DataQualityApiService.findMemberIssues({
    type: props.type,
    limit: limit.value,
    offset: offset.value,
  }, [props.projectGroup])
    .then((res) => {
      if (offset.value > 0) {
        members.value = [...members.value, ...res];
      } else {
        members.value = res;
      }
      if (res.length < limit.value) {
        total.value = members.value.length;
      } else {
        total.value = members.value.length + 1;
      }
    })
    .finally(() => {
      loading.value = false;
    });
};

const loadMore = () => {
  offset.value = members.value.length;
  loadDataIssues();
};

const trackReviewProfile = (memberId: string) => {
  trackEvent({
    key: FeatureEventKey.COPILOT_REVIEW_PROFILE,
    type: EventType.FEATURE,
    properties: {
      issueType: props.type,
      memberId,
      projectGroup: selectedProjectGroup.value?.id,
    },
  });
};

watch(() => props.type, () => {
  offset.value = 0;
  loadDataIssues();
});

watch(() => props.projectGroup, () => {
  offset.value = 0;
  loadDataIssues();
});

onMounted(() => {
  loadDataIssues();
});
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityMemberIssues',
};
</script>
