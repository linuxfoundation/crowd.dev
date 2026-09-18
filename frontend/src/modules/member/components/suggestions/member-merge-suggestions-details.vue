<template>
  <section v-if="props.loading">
    <div
      class="rounded p-6 transition pb-40"
      :class="{ 'bg-gray-50': props.isPrimary }"
    >
      <app-loading height="48px" width="48px" radius="50%" class="mb-6" />

      <app-loading height="16px" width="172px" radius="3px" class="mb-6" />

      <app-loading height="12px" width="100%" radius="3px" class="mb-3" />
      <app-loading height="12px" width="80%" radius="3px" class="mb-8" />

      <app-loading height="12px" width="50%" radius="3px" class="mb-3" />
      <app-loading height="12px" width="50%" radius="3px" class="mb-3" />
      <app-loading height="12px" width="50%" radius="3px" class="mb-3" />
      <app-loading height="12px" width="50%" radius="3px" class="mb-3" />
      <app-loading height="12px" width="50%" radius="3px" class="mb-3" />
    </div>
  </section>
  <section v-else class="h-full">
    <div
      class="rounded p-6 transition h-full"
      :class="{ 'bg-gray-50': props.isPrimary }"
    >
      <!-- primary member -->
      <slot name="header">
        <div class="h-13 flex justify-between items-start">
          <div
            v-if="props.isPreview"
            class="bg-primary-800 rounded-full py-0.5 px-2 text-white inline-block text-xs leading-5 font-medium"
          >
            Preview
          </div>
          <div
            v-else-if="props.isPrimary"
            class="bg-primary-100 rounded-full py-0.5 px-2 text-primary-800 inline-block text-xs leading-5 font-medium"
          >
            Primary profile
          </div>
          <lf-button
            v-else
            type="secondary-gray"
            size="small"
            class="leading-5 !px-4 !py-1"
            @click="emit('makePrimary')"
          >
            <lf-icon name="arrow-right-arrow-left" type="solid" :size="16" class="text-gray-600 mr-2" />
            <span>Make primary</span>
          </lf-button>
          <slot name="action" />
        </div>
      </slot>
      <div class="flex justify-between">
        <router-link
          v-if="!isPreview"
          :to="{
            name: 'memberView',
            params: { id: member.id },
            query: { projectGroup: selectedProjectGroup?.id },
          }"
          target="_blank"
        >
          <div class="relative">
            <app-avatar
              :entity="member"
              class="mb-3"
            />
            <el-tooltip
              v-if="member.attributes?.avatarUrl?.default && getAttributeSourceName(member.attributes?.avatarUrl)"
              :content="`Source: ${getAttributeSourceName(member.attributes?.avatarUrl)}`"
              placement="top"
              trigger="hover"
            >
              <div
                class="absolute top-0 right-[-6px] z-10 h-4 w-4 rounded-full flex items-center justify-center"
                :class="{
                  'bg-white': !props.isPrimary,
                  'bg-gray-50': props.isPrimary,
                }"
              >
                <lf-svg name="source" class="h-3 w-3" />
              </div>
            </el-tooltip>
          </div>
        </router-link>
        <app-avatar
          v-else
          :entity="member"
          class="mb-3"
        />
      </div>
      <div class="pb-6">
        <router-link
          v-if="!isPreview"
          :to="{
            name: 'memberView',
            params: { id: member.id },
            query: { projectGroup: selectedProjectGroup?.id },
          }"
          target="_blank"
        >
          <h6
            class="text-base text-black font-semibold hover:text-primary-500"
            v-html="$sanitize(member.displayName)"
          />
        </router-link>
        <h6
          v-else
          class="text-base text-black font-semibold leading-6"
          v-html="$sanitize(member.displayName)"
        />
        <div class="flex mt-2 items-center gap-1.5">
          <lf-badge v-if="isTeamMember(member)" size="small">
            Team
          </lf-badge>
          <lf-badge v-if="isBot(member)" type="tertiary" size="small">
            Bot
          </lf-badge>
        </div>
        <div class="flex items-center">
          <div
            v-if="member.attributes?.bio?.default"
            ref="bio"
            class="text-gray-600 leading-5 !text-xs merge-member-bio mt-2"
            :class="{ 'line-clamp-2': !more }"
            v-html="$sanitize(member.attributes?.bio.default)"
          />
          <div
            v-else-if="compareMember?.attributes?.bio?.default"
            ref="bio"
            class="text-transparent invisible leading-5 !text-xs merge-member-bio line-clamp-2 mt-2"
            v-html="$sanitize(compareMember?.attributes?.bio.default)"
          />
          <el-tooltip
            v-if="!isPreview && member.attributes?.bio?.default && getAttributeSourceName(member.attributes?.bio)"
            :content="`Source: ${getAttributeSourceName(member.attributes?.bio)}`"
            placement="top"
            trigger="hover"
          >
            <div class="ml-1">
              <lf-svg name="source" class="h-3 w-3" />
            </div>
          </el-tooltip>
        </div>

        <div
          v-if="displayShowMore"
          class="text-sm text-primary-500 mt-2 cursor-pointer"
          :class="{ invisible: !member.attributes?.bio?.default }"
          @click.stop="more = !more"
        >
          Show {{ more ? 'less' : 'more' }}
        </div>
      </div>

      <div>
        <article class="pb-4">
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Engagement level
          </p>
          <slot name="engagementLevel">
            <app-community-engagement-level v-if="member.reach?.total >= 0 && member.score" :member="member" />
            <span v-else class="text-2xs">-</span>
          </slot>
        </article>
        <article
          v-if="
            member.attributes?.location?.default
              || compareMember?.attributes?.location?.default
          "
          class="pb-4"
        >
          <div class="flex items-center pb-1">
            <p class="text-2xs font-medium text-gray-500 mr-1">
              Location
            </p>
            <el-tooltip
              v-if="!isPreview && member.attributes?.location?.default && getAttributeSourceName(member.attributes?.location)"
              :content="`Source: ${getAttributeSourceName(member.attributes?.location)}`"
              placement="top"
              trigger="hover"
            >
              <lf-svg name="source" class="h-3 w-3" />
            </el-tooltip>
          </div>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ member.attributes?.location?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            member.organizations?.length || compareMember?.organizations?.length
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Organization
          </p>
          <div>
            <app-member-organizations :member="member" :show-title="false" />
          </div>
        </article>
        <article
          v-if="
            member.attributes?.jobTitle?.default
              || compareMember?.attributes?.jobTitle?.default
          "
          class="pb-4"
        >
          <div class="flex items-center pb-1">
            <p class="text-2xs font-medium text-gray-500 pr-1">
              Title
            </p>
            <el-tooltip
              v-if="!isPreview && member.attributes?.jobTitle?.default && getAttributeSourceName(member.attributes?.jobTitle)"
              :content="`Source: ${getAttributeSourceName(member.attributes?.jobTitle)}`"
              placement="top"
              trigger="hover"
            >
              <lf-svg name="source" class="h-3 w-3" />
            </el-tooltip>
          </div>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ member.attributes?.jobTitle?.default || '-' }}
          </p>
        </article>
        <article
          v-if="member.joinedAt || compareMember?.joinedAt"
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Joined date
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ formatJoinedDate(member.joinedAt) }}
          </p>
        </article>
        <article
          v-if="member.activityCount > 0"
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Activity Count
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ member.activityCount || 0 }}
          </p>
        </article>
        <slot name="property" />
      </div>
      <div class="pt-4">
        <h6 class="text-sm font-semibold text-black pb-3">
          Identities
        </h6>
        <app-identities-vertical-list-members
          :member="member"
          :order="memberOrder.suggestions"
          :include-emails="true"
        />
      </div>
      <slot name="below" />
    </div>
  </section>
</template>

<script setup>
import {
  onMounted, ref,
} from 'vue';
import AppMemberOrganizations from '@/modules/member/components/member-organizations.vue';
import AppAvatar from '@/shared/avatar/avatar.vue';
import AppCommunityEngagementLevel from '@/modules/member/components/member-engagement-level.vue';
import AppLoading from '@/shared/loading/loading-placeholder.vue';
import memberOrder from '@/shared/modules/identities/config/identitiesOrder/member';
import AppIdentitiesVerticalListMembers from '@/shared/modules/identities/components/identities-vertical-list-members.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfSvg from '@/shared/svg/svg.vue';
import { getAttributeSourceName } from '@/shared/helpers/attribute.helpers';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps({
  member: {
    type: Object,
    default: () => null,
  },
  compareMember: {
    type: Object,
    required: false,
    default: () => null,
  },
  isPrimary: {
    type: Boolean,
    required: false,
    default: false,
  },
  isPreview: {
    type: Boolean,
    required: false,
    default: false,
  },
  loading: {
    type: Boolean,
    required: false,
    default: false,
  },
  extendBio: {
    type: Number,
    required: false,
    default: 0,
  },
});

const emit = defineEmits(['makePrimary', 'bioHeight']);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);
const { isBot, isTeamMember } = useContributorHelpers();

const bio = ref(null);
const displayShowMore = ref(null);
const more = ref(null);

onMounted(() => {
  setTimeout(() => {
    if (!bio.value) {
      return;
    }
    const height = bio.value.clientHeight;
    const { scrollHeight } = bio.value;
    displayShowMore.value = scrollHeight > height || false;
    emit('bioHeight', scrollHeight);
  }, 0);
});

const formatJoinedDate = (date) => {
  if (!date || new Date(date).getFullYear() <= 1970) {
    return '-';
  }
  return dateHelper(date).format('YYYY-MM-DD');
};

defineExpose({
  more,
});
</script>

<script>
export default {
  name: 'AppMemberMergeSuggestionsDetails',
};
</script>

<style lang="scss">
.merge-member-bio * {
  @apply text-xs;
}
</style>
