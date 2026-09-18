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
            Primary organization
          </div>
          <el-tooltip
            v-else
            content="Linux Foundation's member organization must be the primary organization."
            :disabled="!props.compareOrganization.lfxMembership"
            placement="top"
          >
            <span>
              <lf-button
                type="bordered"
                size="small"
                class="leading-5 !px-4 !py-1"
                :disabled="!!props.compareOrganization.lfxMembership"
                @click="emit('makePrimary')"
              >
                <lf-icon name="arrow-right-arrow-left" type="solid" :size="16" class="text-gray-600 mr-2" />
                <span>Make primary</span>
              </lf-button>
            </span>
          </el-tooltip>
          <slot name="action" />
        </div>
      </slot>
      <div class="pb-6">
        <div class="flex justify-between">
          <router-link
            v-if="!isPreview && props.organization?.id"
            :to="{
              name: 'organizationView',
              params: { id: organization.id },
              query: { projectGroup: selectedProjectGroup?.id },
            }"
            target="_blank"
          >
            <lf-avatar
              :name="displayName(props.organization)?.replace('@', '')"
              :src="logo(props.organization)"
              :size="48"
              class="mr-4 mb-4"
              img-class="!object-contain"
            />
          </router-link>
          <lf-avatar
            v-else
            :name="displayName(props.organization)?.replace('@', '')"
            :src="logo(props.organization)"
            :size="48"
            class="mr-4 mb-4"
            img-class="!object-contain"
          />
        </div>
        <div>
          <router-link
            v-if="!isPreview && props.organization.id"
            :to="{
              name: 'organizationView',
              params: { id: organization.id },
              query: { projectGroup: selectedProjectGroup?.id },
            }"
            target="_blank"
          >
            <div class="flex items-center gap-1">
              <h6
                class="text-base text-black font-semibold hover:text-primary-500 leading-6"
                v-html="$sanitize(displayName(props.organization))"
              />
              <lf-organization-lf-member-tag
                :organization="props.organization"
                :only-show-icon="true"
              />
            </div>
          </router-link>
          <div v-else class="flex items-center gap-1">
            <h6
              class="text-base text-black font-semibold hover:text-primary-500 leading-6"
              v-html="$sanitize(displayName(props.organization))"
            />
            <lf-organization-lf-member-tag
              :organization="props.organization"
              :only-show-icon="true"
            />
          </div>
          <div
            v-if="props.organization.attributes?.description?.default"
            ref="bio"
            class="text-gray-600 leading-5 !text-xs merge-member-bio mt-2"
            :class="{ 'line-clamp-2': !more }"
            v-html="$sanitize(props.organization.attributes?.description?.default)"
          />
          <div
            v-else-if="compareOrganization?.attributes?.description?.default"
            ref="bio"
            class="text-transparent invisible leading-5 !text-xs merge-member-bio line-clamp-2 mt-2"
            v-html="$sanitize(compareOrganization?.attributes?.description?.default)"
          />

          <div
            v-if="displayShowMore"
            class="text-sm text-primary-500 mt-2 cursor-pointer"
            :class="{ invisible: !props.organization.description }"
            @click.stop="more = !more"
          >
            Show {{ more ? 'less' : 'more' }}
          </div>
        </div>
      </div>

      <div>
        <article
          v-if="
            getOrganizationWebsite(organization)
              || getOrganizationWebsite(compareOrganization)
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Website
          </p>
          <a
            :href="withHttp(getOrganizationWebsite(organization))"
            target="_blank"
            rel="noopener noreferrer"
            class="text-xs text-gray-900 whitespace-normal inline-block leading"
          >{{ getOrganizationWebsite(organization) || '-' }}</a>
        </article>
        <article
          v-if="
            props.organization.location
              || props.compareOrganization?.location
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Location
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ props.organization.location || '-' }}
          </p>
        </article>
        <article
          v-if="
            props.organization.attributes?.employees?.default
              || props.compareOrganization?.attributes?.employees?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            # of employees
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ props.organization.attributes?.employees?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            props.organization.attributes?.revenueRange?.default
              || props.compareOrganization?.attributes?.revenueRange?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Annual Revenue
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ revenueRange.formatValue(
              props.organization.attributes?.revenueRange?.default,
            ) || '-' }}
          </p>
        </article>
        <article
          v-if="
            props.organization.attributes?.industry?.default
              || props.compareOrganization?.attributes?.industry?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Industry
          </p>
          <p class="text-xs text-gray-900 first-letter:uppercase whitespace-normal">
            {{ props.organization.attributes?.industry?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            props.organization.attributes?.type?.default
              || props.compareOrganization?.attributes?.type?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Type
          </p>
          <p class="text-xs text-gray-900 first-letter:uppercase whitespace-normal">
            {{ props.organization.attributes?.type?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            props.organization.attributes?.founded?.default
              || props.compareOrganization?.attributes?.founded?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Founded
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ props.organization.attributes?.founded?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            props.organization.joinedAt
              || props.compareOrganization?.joinedAt
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Joined date
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ formatDateToTimeAgo(props.organization.joinedAt) || '-' }}
          </p>
        </article>
        <article
          v-if="
            props.organization.memberCount
              || props.compareOrganization?.memberCount
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            # of people
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ props.organization.memberCount || '-' }}
          </p>
        </article>
        <article
          v-if="
            props.organization.activityCount
              || props.compareOrganization?.activityCount
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            # of Activities
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ props.organization.activityCount || '-' }}
          </p>
        </article>
      </div>
      <div class="pt-4">
        <h6 class="text-sm font-semibold pb-3">
          Identities
        </h6>
        <app-identities-vertical-list-organizations
          :organization="organization"
          :include-emails="true"
          :include-domains="true"
          :include-phone-numbers="true"
          :order="organizationOrder.suggestions"
        />
      </div>
    </div>
  </section>
</template>

<script setup>
import {
  onMounted, ref,
} from 'vue';
import AppLoading from '@/shared/loading/loading-placeholder.vue';
import { withHttp } from '@/utils/string';
import { formatDateToTimeAgo } from '@/utils/date';
import revenueRange from '@/modules/organization/config/enrichment/revenueRange';
import AppIdentitiesVerticalListOrganizations from '@/shared/modules/identities/components/identities-vertical-list-organizations.vue';
import organizationOrder from '@/shared/modules/identities/config/identitiesOrder/organization';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfOrganizationLfMemberTag from '@/modules/organization/components/lf-member/organization-lf-member-tag.vue';
import { getOrganizationWebsite } from '@/utils/organization';
import useOrganizationHelpers from '@/modules/organization/helpers/organization.helpers';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps({
  organization: {
    type: Object,
    default: () => null,
  },
  compareOrganization: {
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

const { logo, displayName } = useOrganizationHelpers();

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

defineExpose({
  more,
});
</script>

<script>
export default {
  name: 'AppOrganizationMergeSuggestionsDetails',
};
</script>

<style lang="scss">
.merge-member-bio * {
  @apply text-xs;
}
</style>
