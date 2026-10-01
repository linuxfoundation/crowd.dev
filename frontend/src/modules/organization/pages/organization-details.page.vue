<template>
  <div v-if="loading" class="flex justify-center py-20">
    <lf-spinner />
  </div>
  <lf-not-found-page
    v-else-if="!organization?.id"
    :back-message="'Back to organizations'"
    :to="{ path: '/organizations' }"
  />
  <div v-else class="-mt-5 -mb-5">
    <div class="organization-details grid grid-cols-2 grid-rows-2 px-3">
      <section
        class="w-full border-b border-gray-100 py-4 flex justify-between items-center col-span-2 h-min"
        :class="hovered ? 'is-hovered' : ''"
        @mouseover="hovered = true"
        @mouseout="hovered = false"
      >
        <div class="flex items-center flex-grow">
          <lf-back
            :to="{ path: hasSegments ? '/organizations' : '/admin' }"
            class="mr-2"
            @mouseover.stop
            @mouseout.stop
          >
            <lf-button type="secondary-ghost" icon-only>
              <lf-icon name="chevron-left" />
            </lf-button>
          </lf-back>
          <lf-organization-details-header :organization="organization" />
        </div>
        <div class="flex items-center">
          <lf-organization-syncing-activities
            v-if="
              organization.activitySycning?.state
                === MergeActionState.IN_PROGRESS
            "
            :organization="organization"
            class="mr-4"
          />
          <lf-organization-last-enrichment
            v-else
            :organization="organization"
            class="mr-4"
          />
          <div @mouseover.stop @mouseout.stop>
            <lf-organization-details-actions
              :organization="organization"
              @reload="fetchOrganization(id as string)"
            />
          </div>
        </div>
      </section>
      <section
        class="w-80 border-r relative border-gray-100 overflow-y-auto overflow-x-visible h-full"
      >
        <div
          class="sticky top-0 left-0 w-full h-6 bg-gradient-to-b from-white to-transparent"
        />
        <div class="pr-8 pb-10">
          <lf-organization-details-identities
            :organization="organization"
            class="mb-8"
            @reload="fetchOrganization(id as string)"
          />
          <lf-organization-details-domains
            :organization="organization"
            class="mb-8"
            @reload="fetchOrganization(id as string)"
          />
          <lf-organization-details-emails
            :organization="organization"
            class="mb-8"
            @reload="fetchOrganization(id as string)"
          />
          <lf-organization-details-phone-numbers
            :organization="organization"
            @reload="fetchOrganization(id as string)"
          />
        </div>
      </section>
      <section
        ref="scrollContainer"
        class="overflow-auto h-full pb-10"
        @scroll="controlScroll"
      >
        <div class="sticky top-0 z-10">
          <div class="bg-white pt-5 pl-10 pb-3">
            <lf-tabs v-model="tabs" @update:model-value="handleTabChange">
              <lf-tab v-model="tabs" name="overview">
                Overview
              </lf-tab>
              <lf-tab v-model="tabs" name="people">
                People
              </lf-tab>
              <lf-tab v-model="tabs" name="activities">
                <div class="flex items-center gap-1">
                  Activities
                  <lf-icon
                    v-if="
                      organization.activitySycning?.state
                        === MergeActionState.ERROR
                    "
                    name="circle-exclamation"
                    :size="16"
                    class="text-red-500"
                  />
                </div>
              </lf-tab>
            </lf-tabs>
          </div>
          <div
            class="w-full h-5 bg-gradient-to-b from-white to-transparent pl-10"
          />
        </div>
        <div class="pl-10">
          <lf-organization-details-overview
            v-if="tabs === 'overview'"
            :organization="organization"
          />
          <lf-organization-details-contributors
            v-else-if="tabs === 'people'"
            ref="contributors"
            :organization="organization"
          />
          <lf-organization-details-activities
            v-else-if="tabs === 'activities'"
            ref="activities"
            :organization="organization"
          />
        </div>
      </section>
    </div>
  </div>
</template>

<script setup lang="ts">
import LfTabs from '@/ui-kit/tabs/Tabs.vue';
import LfTab from '@/ui-kit/tabs/Tab.vue';
import { computed, onMounted, ref } from 'vue';
import LfBack from '@/ui-kit/back/Back.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { useRoute } from 'vue-router';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfOrganizationDetailsHeader from '@/modules/organization/components/details/organization-details-header.vue';
import LfOrganizationLastEnrichment from '@/modules/organization/components/shared/organization-last-enrichment.vue';
import LfOrganizationDetailsActions from '@/modules/organization/components/details/organization-details-actions.vue';
import LfOrganizationDetailsOverview from '@/modules/organization/components/details/organization-details-overview.vue';
import LfOrganizationDetailsActivities from '@/modules/organization/components/details/organization-details-activities.vue';
import LfOrganizationDetailsIdentities from '@/modules/organization/components/details/organization-details-identities.vue';
import LfOrganizationDetailsDomains from '@/modules/organization/components/details/organization-details-domains.vue';
import LfOrganizationDetailsPhoneNumbers from '@/modules/organization/components/details/organization-details-phone-numbers.vue';
import LfOrganizationDetailsEmails from '@/modules/organization/components/details/organization-details-emails.vue';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import LfOrganizationDetailsContributors from '@/modules/organization/components/details/organization-details-contributors.vue';
import LfOrganizationSyncingActivities from '@/modules/organization/components/shared/organization-syncing-activities.vue';
import { MergeActionState } from '@/shared/modules/merge/types/MemberActions';
import LfNotFoundPage from '@/shared/not-found-page/lf-not-found-page.vue';

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const route = useRoute();

const tabs = ref('overview');
const contributors = ref<any>(null);
const activities = ref<any>(null);
const scrollContainer = ref<any>(null);

const { id } = route.params;

const organizationStore = useOrganizationStore();
const { organization } = storeToRefs(organizationStore);
const { fetchOrganization } = organizationStore;

const hovered = ref<boolean>(false);

const loading = ref<boolean>(false);
const getOrganization = () => {
  if (!organization.value) {
    loading.value = true;
  }
  fetchOrganization(id as string).finally(() => {
    loading.value = false;
  });
};

const controlScroll = (e: any) => {
  if (
    e.target.scrollTop + e.target.clientHeight
    >= e.target.scrollHeight - 10
  ) {
    if (tabs.value === 'people') {
      contributors.value.loadMore();
    } else if (tabs.value === 'activities') {
      activities.value.loadMore();
    }
  }
};

const handleTabChange = () => {
  if (scrollContainer.value) {
    scrollContainer.value.scrollTop = 0;
  }
};

const hasSegments = computed(() => selectedProjectGroup.value?.id);

onMounted(() => {
  organization.value = null;
  getOrganization();
});
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsPage',
};
</script>

<style>
.organization-details {
  max-width: 67.5rem;
  height: calc(100vh - 4.25rem);
  grid-template-rows: min-content auto;
  grid-template-columns: 20rem auto;
  @apply w-full mx-auto;
}
</style>
