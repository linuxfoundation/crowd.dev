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
            Primary company
          </div>
          <el-tooltip
            v-else
            content="Linux Foundation's member company must be the primary company."
            :disabled="!props.compareOrganization!.lfxMembership"
            placement="top"
          >
            <span>
              <lf-button
                type="bordered"
                size="small"
                class="leading-5 !px-4 !py-1"
                :disabled="!!props.compareOrganization!.lfxMembership"
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
            v-if="!isPreview && currentOrganization?.id"
            :to="{
              name: 'organizationView',
              params: { id: currentOrganization.id },
              query: { projectGroup: selectedProjectGroup?.id },
            }"
            target="_blank"
          >
            <lf-avatar
              :name="displayName(currentOrganization)?.replace('@', '')"
              :src="logo(currentOrganization)"
              :size="48"
              class="mr-4 mb-4"
              img-class="!object-contain"
            />
          </router-link>
          <lf-avatar
            v-else
            :name="displayName(currentOrganization)?.replace('@', '')"
            :src="logo(currentOrganization)"
            :size="48"
            class="mr-4 mb-4"
            img-class="!object-contain"
          />
        </div>
        <div>
          <router-link
            v-if="!isPreview && currentOrganization.id"
            :to="{
              name: 'organizationView',
              params: { id: currentOrganization.id },
              query: { projectGroup: selectedProjectGroup?.id },
            }"
            target="_blank"
          >
            <div class="flex items-center gap-1">
              <h6
                class="text-base text-black font-semibold hover:text-primary-500 leading-6"
                v-html="$sanitize(displayName(currentOrganization))"
              />
              <lf-organization-lf-member-tag
                :organization="currentOrganization"
                only-show-icon
              />
            </div>
          </router-link>
          <div v-else class="flex items-center gap-1">
            <h6
              class="text-base text-black font-semibold hover:text-primary-500 leading-6"
              v-html="$sanitize(displayName(currentOrganization))"
            />
            <lf-organization-lf-member-tag
              :organization="currentOrganization"
              only-show-icon
            />
          </div>
          <div
            v-if="currentOrganization.attributes?.description?.default"
            ref="bio"
            class="text-gray-600 leading-5 !text-xs merge-member-bio mt-2"
            :class="{ 'line-clamp-2': !more }"
            v-html="$sanitize(currentOrganization.attributes?.description?.default)"
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
            :class="{ invisible: !currentOrganization.description }"
            @click.stop="more = !more"
          >
            Show {{ more ? 'less' : 'more' }}
          </div>
        </div>
      </div>

      <div>
        <article
          v-if="
            getOrganizationWebsite(currentOrganization)
              || getOrganizationWebsite(compareOrganization)
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Website
          </p>
          <a
            :href="withHttp(getOrganizationWebsite(currentOrganization) as string)"
            target="_blank"
            rel="noopener noreferrer"
            class="text-xs text-gray-900 whitespace-normal inline-block leading"
          >{{ getOrganizationWebsite(currentOrganization) || '-' }}</a>
        </article>
        <article
          v-if="
            currentOrganization.location
              || props.compareOrganization?.location
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Location
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ currentOrganization.location || '-' }}
          </p>
        </article>
        <article
          v-if="
            currentOrganization.attributes?.employees?.default
              || props.compareOrganization?.attributes?.employees?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            # of employees
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ currentOrganization.attributes?.employees?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            currentOrganization.attributes?.revenueRange?.default
              || props.compareOrganization?.attributes?.revenueRange?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Annual Revenue
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ revenueRange.formatValue(
              currentOrganization.attributes?.revenueRange?.default,
            ) || '-' }}
          </p>
        </article>
        <article
          v-if="
            currentOrganization.attributes?.industry?.default
              || props.compareOrganization?.attributes?.industry?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Industry
          </p>
          <p class="text-xs text-gray-900 first-letter:uppercase whitespace-normal">
            {{ currentOrganization.attributes?.industry?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            currentOrganization.attributes?.type?.default
              || props.compareOrganization?.attributes?.type?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Type
          </p>
          <p class="text-xs text-gray-900 first-letter:uppercase whitespace-normal">
            {{ currentOrganization.attributes?.type?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            currentOrganization.attributes?.founded?.default
              || props.compareOrganization?.attributes?.founded?.default
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Founded
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ currentOrganization.attributes?.founded?.default || '-' }}
          </p>
        </article>
        <article
          v-if="
            currentOrganization.joinedAt
              || props.compareOrganization?.joinedAt
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            Joined date
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ formatDateToTimeAgo(currentOrganization.joinedAt) || '-' }}
          </p>
        </article>
        <article
          v-if="
            currentOrganization.memberCount
              || props.compareOrganization?.memberCount
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            # of people
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ currentOrganization.memberCount || '-' }}
          </p>
        </article>
        <article
          v-if="
            currentOrganization.activityCount
              || props.compareOrganization?.activityCount
          "
          class="pb-4"
        >
          <p class="text-2xs font-medium text-gray-500 pb-1">
            # of Activities
          </p>
          <p class="text-xs text-gray-900 whitespace-normal">
            {{ currentOrganization.activityCount || '-' }}
          </p>
        </article>
      </div>
      <div class="pt-4">
        <h6 class="text-sm font-semibold pb-3">
          Identities
        </h6>
        <app-identities-vertical-list-organizations
          :organization="currentOrganization"
          include-emails
          include-domains
          include-phone-numbers
          :order="organizationOrder.suggestions"
        />
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import {
  computed, onMounted, ref,
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
import type { Organization } from '@/modules/organization/types/Organization';

defineOptions({ name: 'AppOrganizationMergeSuggestionsDetails' });

const props = withDefaults(defineProps<{
  organization?: Partial<Organization> | null;
  compareOrganization?: Organization | null;
  isPrimary?: boolean;
  isPreview?: boolean;
  loading?: boolean;
  extendBio?: number;
}>(), {
  organization: () => null,
  compareOrganization: () => null,
  isPrimary: false,
  isPreview: false,
  loading: false,
  extendBio: 0,
});

const emit = defineEmits<{(e: 'makePrimary'): void;
  (e: 'bioHeight', value: number): void;
}>();

defineSlots<{
  header?:() => unknown;
  action?:() => unknown;
}>();

// shortcut: organization is only null while loading. revisit: K-20 / CM-1731 moves the loading skeleton out
const currentOrganization = computed(() => props.organization!);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const { logo, displayName } = useOrganizationHelpers();

const bio = ref<HTMLElement | null>(null);
const displayShowMore = ref<boolean | null>(null);
const more = ref<boolean | null>(null);

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

// eslint-disable-next-line vue/define-macros-order -- plain <script> below counts as last, https://eslint.vuejs.org/rules/define-macros-order.html
defineExpose({
  more,
});
</script>

<style lang="scss">
.merge-member-bio * {
  @apply text-xs;
}
</style>
