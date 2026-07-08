<template>
  <app-page-wrapper>
    <div class="pb-40">
      <app-back-link
        :default-route="{
          path: '/organizations',
          query: { projectGroup: selectedProjectGroup?.id },
        }"
        class="font-semibold"
      >
        <template #default>
          Organizations
        </template>
      </app-back-link>
      <div class="flex items-center pb-6">
        <h4 class="text-xl font-semibold leading-9">
          Merge suggestions <span v-if="totalCount" class="font-light text-gray-500">({{ totalCount }})</span>
        </h4>
        <el-tooltip
          placement="top"
          content="LFX is constantly checking your community for duplicate organizations. Here you can check all the merging suggestions."
        >
          <lf-icon name="circle-question" :size="20" class="text-gray-500 flex items-center ml-2 h-5" />
        </el-tooltip>
      </div>

      <app-merge-suggestions-filters placeholder="Search organizations" @search="search" />

      <div
        v-if="page <= 1 && loading && mergeSuggestions.length === 0"
        class="flex justify-center pt-8"
      >
        <lf-spinner />
      </div>

      <lf-table v-else-if="mergeSuggestions.length > 0" class="mt-6">
        <thead>
          <tr>
            <lf-table-head colspan="2">
              Organizations
            </lf-table-head>
            <lf-table-head v-model="sorting" property="similarity" @update:model-value="() => loadMergeSuggestions(true)">
              Confidence level
            </lf-table-head>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(suggestion, si) of mergeSuggestions" :key="si">
            <td>
              <div class="flex items-center">
                <router-link
                  :to="{
                    name: 'organizationView',
                    params: { id: suggestion.organizations[0].id },
                    query: { projectGroup: selectedProjectGroup?.id },
                  }"
                  target="_blank"
                  class="text-black hover:text-primary-500"
                >
                  <div class="flex items-center gap-2">
                    <app-avatar
                      size="xs"
                      :entity="{
                        ...suggestion.organizations[0],
                        avatar: suggestion.organizations[0].logo,
                        displayName: (suggestion.organizations[0].displayName || suggestion.organizations[0].name)?.replace('@', ''),
                      }"
                    />

                    <div class="flex items-center gap-1">
                      <p class="text-xs leading-5 font-semibold truncate max-w-3xs">
                        {{ suggestion.organizations[0].displayName }}
                      </p>

                      <lf-organization-lf-member-tag
                        :organization="suggestion.organizations[0]"
                        :only-show-icon="true"
                      />
                    </div>
                  </div>
                </router-link>
              </div>
            </td>
            <td>
              <div class="flex items-center">
                <router-link
                  :to="{
                    name: 'organizationView',
                    params: { id: suggestion.organizations[1].id },
                    query: { projectGroup: selectedProjectGroup?.id },
                  }"
                  target="_blank"
                  class="text-black hover:text-primary-500"
                >
                  <div class="flex items-center gap-2">
                    <lf-icon name="minus" :size="24" class="text-gray-300" />
                    <app-avatar
                      size="xs"
                      :entity="{
                        ...suggestion.organizations[1],
                        avatar: suggestion.organizations[1].logo,
                        displayName: (suggestion.organizations[1].displayName || suggestion.organizations[1].name)?.replace('@', ''),
                      }"
                    />

                    <div class="flex items-center gap-1">
                      <p class="text-xs leading-5 font-semibold truncate max-w-3xs">
                        {{ suggestion.organizations[1].displayName }}
                      </p>

                      <lf-organization-lf-member-tag
                        :organization="suggestion.organizations[1]"
                        :only-show-icon="true"
                      />
                    </div>
                  </div>
                </router-link>
              </div>
            </td>
            <td>
              <app-member-merge-similarity :similarity="suggestion.similarity" />
            </td>
            <td class="w-48">
              <div class="flex justify-end items-center gap-3">
                <lf-button size="small" type="primary-ghost" @click="openDetails(si)">
                  View suggestion
                </lf-button>
                <lf-dropdown placement="bottom-end" width="15rem">
                  <template #trigger>
                    <lf-button
                      size="small"
                      type="secondary-ghost-light"
                      :loading="sending === `${suggestion.organizations[0].id}:${suggestion.organizations[1].id}`"
                      :icon-only="true"
                    >
                      <lf-icon name="ellipsis" type="regular" />
                    </lf-button>
                  </template>

                  <lf-dropdown-item @click="merge(suggestion)">
                    <lf-icon name="shuffle" /> Merge suggestion
                  </lf-dropdown-item>

                  <lf-dropdown-item @click="ignore(suggestion)">
                    <lf-icon name="circle-xmark" />Ignore suggestion
                  </lf-dropdown-item>
                </lf-dropdown>
              </div>
            </td>
          </tr>
        </tbody>
      </lf-table>
      <div v-else class="py-20 flex flex-col items-center">
        <lf-icon name="shuffle" :size="160" class="text-gray-200 flex items-center mb-8" />
        <h5 class="text-center text-lg font-semibold mb-4">
          No merge suggestions
        </h5>
        <p class="text-sm text-center text-gray-600 leading-5">
          We couldn’t find any duplicated organizations
        </p>
      </div>

      <div v-if="hasMore" class="mt-6 flex justify-center">
        <lf-button type="primary-ghost" size="small" :loading="loading" @click="loadMore()">
          <lf-icon name="arrow-down" />Load more
        </lf-button>
      </div>
    </div>
  </app-page-wrapper>
  <app-organization-merge-suggestions-dialog
    v-model="isModalOpen"
    :offset="detailsOffset"
    :query="{
      filter,
      orderBy: [sorting],
    }"
    @reload="reload()"
  />
</template>

<script setup lang="ts">
import AppBackLink from '@/shared/modules/back-link/components/back-link.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { onMounted, ref } from 'vue';

import { ToastStore } from '@/shared/message/notification';
import { OrganizationService } from '@/modules/organization/organization-service';
import LfButton from '@/ui-kit/button/Button.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import AppMemberMergeSimilarity from '@/modules/member/components/suggestions/member-merge-similarity.vue';
import AppAvatar from '@/shared/avatar/avatar.vue';
import AppOrganizationMergeSuggestionsDialog
  from '@/modules/organization/components/organization-merge-suggestions-dialog.vue';
import useOrganizationMergeMessage from '@/shared/modules/merge/config/useOrganizationMergeMessage';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import AppMergeSuggestionsFilters from '@/modules/member/components/suggestions/merge-suggestions-filters.vue';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import LfOrganizationLfMemberTag from '@/modules/organization/components/lf-member/organization-lf-member-tag.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const mergeSuggestions = ref<any[]>([]);

const isModalOpen = ref<boolean>(false);

const hasMore = ref<boolean>(false);
const limit = ref<number>(10);
const page = ref<number>(1);
const loading = ref<boolean>(false);
const sorting = ref<string>('similarity_DESC');
const totalCount = ref<number>(0);

const filter = ref<any>(undefined);

const { trackEvent } = useProductTracking();

const loadMergeSuggestions = (sort: boolean = false) => {
  if (sort) {
    trackEvent({
      key: FeatureEventKey.SORT_ORGANIZATIONS_MERGE_SUGGESTIONS,
      type: EventType.FEATURE,
      properties: {
        orderBy: [sorting.value],
      },
    });
  }

  loading.value = true;
  OrganizationService.fetchMergeSuggestions(limit.value, (page.value - 1) * limit.value, {
    filter: filter.value,
    orderBy: [sorting.value],
    detail: false,
  })
    .then((res) => {
      hasMore.value = Boolean(res.hasMore);
      const rows = res.rows.filter((s: any) => s.similarity > 0);
      if (+res.offset > 0) {
        mergeSuggestions.value = [...mergeSuggestions.value, ...rows];
      } else {
        mergeSuggestions.value = rows;
      }
    })
    .finally(() => {
      loading.value = false;
    });
};

const getTotalCount = () => {
  OrganizationService.fetchMergeSuggestions(0, 0, {
    countOnly: true,
  })
    .then(({ count }) => {
      totalCount.value = count;
    });
};

const detailsOffset = ref<number>(0);

const openDetails = (index: number) => {
  trackEvent({
    key: FeatureEventKey.VIEW_ORGANIZATION_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: mergeSuggestions.value[index].similarity,
    },
  });

  detailsOffset.value = index;
  isModalOpen.value = true;
};

const search = (query: any) => {
  page.value = 1;
  filter.value = query;
  loadMergeSuggestions();
};

const reload = () => {
  page.value = 1;
  loadMergeSuggestions();
  getTotalCount();
};

const loadMore = () => {
  page.value += 1;
  loadMergeSuggestions();
};

const sending = ref<string>('');

const merge = (suggestion: any) => {
  if (sending.value.length) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.MERGE_ORGANIZATION_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: suggestion.similarity,
    },
  });

  const primaryOrganization = suggestion.organizations[0];
  const secondaryOrganization = suggestion.organizations[1];
  sending.value = `${primaryOrganization.id}:${secondaryOrganization.id}`;

  const { loadingMessage, successMessage } = useOrganizationMergeMessage;

  loadingMessage();

  OrganizationService.mergeOrganizations(primaryOrganization.id, secondaryOrganization.id)
    .then(() => {
      successMessage({
        primaryOrganization,
        secondaryOrganization,
      });
    })
    .finally(() => {
      reload();
      sending.value = '';
    });
};

const ignore = (suggestion: any) => {
  if (sending.value.length) {
    return;
  }

  trackEvent({
    key: FeatureEventKey.IGNORE_ORGANIZATION_MERGE_SUGGESTION,
    type: EventType.FEATURE,
    properties: {
      similarity: suggestion.similarity,
    },
  });

  const primaryMember = suggestion.members[0];
  const secondaryMember = suggestion.members[1];
  sending.value = `${primaryMember.id}:${secondaryMember.id}`;
  OrganizationService.addToNoMerge(...suggestion.members)
    .then(() => {
      ToastStore.success('Merging suggestion ignored successfully');
      reload();
    })
    .finally(() => {
      sending.value = '';
    });
};

onMounted(() => {
  getTotalCount();
});
</script>

<script lang="ts">
export default {
  name: 'AppOrganizationMergeSuggestionsPage',
};
</script>
