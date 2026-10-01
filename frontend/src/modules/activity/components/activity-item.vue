<template>
  <div>
    <article v-if="loading || !activity">
      <app-loading height="85px" radius="8px" />
    </article>
    <!-- For now only render the new UI for Git activities -->
    <article v-else-if="activity.platform === Platform.GIT">
      <lf-activity-display
        :activity="activity"
      />
    </article>
    <article v-else class="panel">
      <div class="flex">
        <!-- Avatar -->
        <div class="pr-3">
          <router-link
            v-if="activity.member"
            :to="{
              name: 'memberView',
              params: { id: activity.member.id },
              query: { projectGroup: selectedProjectGroup?.id },
            }"
          >
            <app-avatar
              :entity="activity.member"
              size="xs"
            />
          </router-link>
        </div>
        <!-- activity info -->
        <div class="flex-grow">
          <div class="flex justify-between">
            <div>
              <app-member-display-name
                v-if="activity.member"
                class="flex items-center pb-0.5"
                custom-class="text-2xs leading-4 text-gray-900 font-medium block"
                :member="activity.member"
                with-link
              />
              <div class="flex items-center">
                <div>
                  <!-- platform icon -->
                  <el-tooltip
                    v-if="platform"
                    effect="dark"
                    :content="platform.name"
                    placement="top"
                  >
                    <img
                      :alt="platform.name"
                      class="min-w-[16px] w-4 h-4"
                      :src="platform.image"
                    />
                  </el-tooltip>
                  <lf-icon v-else name="satellite-dish" :size="16" class="text-gray-400" />
                </div>
                <app-activity-header
                  :activity="activity"
                  class="text-xs leading-4 pl-2 flex flex-wrap"
                />
              </div>
            </div>
          </div>
          <!-- member name -->
          <div
            v-if="activity.title || activity.body"
            class="pt-6"
          >
            <app-activity-content
              :activity="activity"
              :display-body="false"
              :display-title="false"
            />
            <app-activity-content
              class="text-sm bg-gray-50 rounded-lg p-4"
              :activity="activity"
              show-more
              :display-thread="false"
            >
              <template v-if="platform?.activityDisplay?.showContentDetails" #details>
                <div v-if="activity.attributes">
                  <app-activity-content-footer
                    :source-id="activity.sourceId"
                    :changes="activity.attributes.lines"
                    changes-copy="line"
                    :insertions="activity.attributes.insertions"
                    :deletions="activity.attributes.deletions"
                    :display-source-id="activity.type === 'authored-commit'"
                  />
                </div>
              </template>

              <template #bottomLink>
                <div v-if="activity.url" class="pt-6">
                  <app-activity-link
                    :activity="activity"
                  />
                </div>
              </template>
            </app-activity-content>
          </div>
        </div>
      </div>
    </article>
  </div>
</template>

<script setup>
import { lfIdentities } from '@/config/identities';
import AppActivityContentFooter from '@/modules/activity/components/activity-content-footer.vue';
import AppActivityContent from '@/modules/activity/components/activity-content.vue';
import AppActivityLink from '@/modules/activity/components/activity-link.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import AppMemberDisplayName from '@/modules/member/components/member-display-name.vue';
import AppAvatar from '@/shared/avatar/avatar.vue';
import AppLoading from '@/shared/loading/loading-placeholder.vue';
import LfActivityDisplay from '@/shared/modules/activity/components/activity-display.vue';
import { Platform } from '@/shared/modules/platform/types/Platform';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { storeToRefs } from 'pinia';
import { computed } from 'vue';
import AppActivityHeader from './activity-header.vue';

const props = defineProps({
  activity: {
    type: Object,
    required: false,
    default: () => ({}),
  },
  loading: {
    type: Boolean,
    required: false,
    default: false,
  },
});

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const platform = computed(() => lfIdentities[props.activity.platform]);

</script>

<script>
export default {
  name: 'AppActivityItem',
};
</script>
