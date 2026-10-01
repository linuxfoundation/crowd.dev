<template>
  <lf-filter
    ref="memberFilter"
    v-model="filters"
    :config="filterConfig"
    :search-config="memberSearchFilter"
    :saved-views-config="memberSavedViews"
    :custom-config="customAttributesFilter"
    hash="people"
    @fetch="onFilterChange($event)"
  />
  <div v-if="contributors.length > 0" class="flex justify-between items-center pb-3 -mt-2">
    <!-- Total number -->
    <p class="text-small text-gray-500">
      {{ pluralize('person', pagination.total, true) }}
    </p>

    <!-- Sorting -->
    <lf-dropdown placement="bottom-end" width="15rem">
      <template #trigger>
        <div class="flex items-center gap-1 py-2">
          <p class="text-small">
            <span class="font-semibold"> Sort: </span>
            {{ sorters[sort] }}
          </p>
          <lf-icon name="chevron-down" :size="16" />
        </div>
      </template>

      <lf-dropdown-item
        v-for="(label, key) in sorters"
        :key="key"
        :class="sort === key ? 'bg-primary-25' : ''"
        class="flex justify-between w-full"
        @click="onSortChange(key)"
      >
        <span>{{ label }}</span>
        <lf-icon v-if="sort === key" name="check" :size="16" class="text-primary-500" />
      </lf-dropdown-item>
    </lf-dropdown>
  </div>

  <!-- Contact list -->
  <div>
    <div v-if="loading && pagination.page <= 1" class="flex justify-center pt-6">
      <lf-spinner />
    </div>
    <div v-else>
      <article
        v-for="contributor of contributors"
        :key="contributor.id"
        class="border-b border-gray-200 last:border-b-0 py-2 flex items-center justify-between"
      >
        <router-link
          :to="{
            name: 'memberView',
            params: { id: contributor.id },
            query: { projectGroup: selectedProjectGroup?.id },
          }"
          class="flex items-center gap-2 group"
        >
          <div
            class="border-2 rounded-full p-0.5"
            :class="isNew(contributor) ? 'border-primary-500' : 'border-transparent'"
          >
            <lf-avatar :src="avatar(contributor)" :name="contributor.displayName" :size="32" />
          </div>
          <p class="text-medium font-semibold text-black group-hover:text-primary-500 transition">
            {{ contributor.displayName }}
          </p>
        </router-link>
        <div class="flex items-center gap-4">
          <p class="text-small text-gray-500 whitespace-nowrap">
            {{ pluralize('activity', contributor.activityCount, true) }}
          </p>
          <lf-tooltip content="Engagement level" :disabled="contributor.score >= 0">
            <lf-contributor-engagement-level :contributor="contributor" />
          </lf-tooltip>

          <app-identities-horizontal-list-members
            :member="contributor"
            :limit="0"
            placement="top-end"
          >
            <template #badge>
              <div>
                <div class="h-6 flex items-center px-2 border border-gray-200 rounded-md gap-1.5 whitespace-nowrap">
                  <lf-icon name="fingerprint" :size="16" />
                  <p class="text-small text-gray-600 whitespace-nowrap">
                    {{ pluralize('identity', identities(contributor).length, true) }}
                  </p>
                </div>
              </div>
            </template>
          </app-identities-horizontal-list-members>
        </div>
      </article>
    </div>
    <div v-if="!loading && !contributors.length">
      <div class="flex justify-center pb-8">
        <lf-icon name="people-group" :size="80" class="text-gray-200" />
      </div>
      <h5 class="text-center text-h5">
        No people found
      </h5>
      <p class="text-gray-600 text-small text-center mt-4">
        We couldn't find any results that match your search criteria, please try a different query
      </p>
    </div>
    <div
      v-if="pagination.total > pagination.page * pagination.perPage"
      class="pt-10 pb-6 gap-4 flex justify-center items-center"
    >
      <p class="text-small text-gray-400">
        {{ contributors.length }} of {{ totalContacts }} people
      </p>
      <lf-button type="primary-ghost" loading-text="Loading people..." :loading="loading" @click="loadMore">
        Load more
      </lf-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Organization } from '@/modules/organization/types/Organization';
import { memberFilters, memberSearchFilter } from '@/modules/member/config/filters/main';
import { memberSavedViews } from '@/modules/member/config/saved-views/main';
import LfFilter from '@/shared/modules/filters/components/Filter.vue';
import { onMounted, ref } from 'vue';
import { storeToRefs } from 'pinia';
import { useMemberStore } from '@/modules/member/store/pinia';
import { MemberService } from '@/modules/member/member-service';
import type { FilterQuery } from '@/shared/modules/filters/types/FilterQuery';
import type { Filter } from '@/shared/modules/filters/types/FilterConfig';
import type { Pagination } from '@/shared/types/Pagination';
import type { Member } from '@/modules/member/types/Member';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfButton from '@/ui-kit/button/Button.vue';
import AppIdentitiesHorizontalListMembers from '@/shared/modules/identities/components/identities-horizontal-list-members.vue';
import pluralize from 'pluralize';
import LfTooltip from '@/ui-kit/tooltip/Tooltip.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfContributorEngagementLevel from '@/modules/contributor/components/shared/contributor-engagement-level.vue';
import useContributorHelpers from '@/modules/contributor/helpers/contributor.helpers';

const props = defineProps<{
  organization: Organization,
}>();

const filterConfig = { ...memberFilters };
delete filterConfig.organizations;

const memberStore = useMemberStore();
const { customAttributesFilter } = storeToRefs(memberStore);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const contributors = ref<Member[]>([]);
const totalContacts = ref<number>(0);
const loading = ref<boolean>(false);

const savedBody = ref<any>({});
const searchStr = ref<string>('');

const {
  avatar, isNew, identities,
} = useContributorHelpers();

const sorters = {
  score_DESC: 'Most engaged',
  activityCount_DESC: 'Most activities',
  displayName_ASC: 'Alphabetically',
};

const sort = ref<string>('score_DESC');

const filters = ref<Filter>({
  search: '',
  relation: 'and',
  order: {
    prop: 'activityCount',
    order: 'descending',
  },
  settings: {
    bot: 'include',
    teamMember: 'include',
    organization: 'include',
  },
});

const pagination = ref({
  page: 1,
  perPage: 20,
  total: 0,
});

const orgFilter = { organizations: { contains: [props.organization.id] } };

const doGetMembersCount = () => {
  MemberService.listMembers(
    {
      limit: 1,
      offset: 0,
      filter: orgFilter,
      segments: selectedProjectGroup.value?.id ? [selectedProjectGroup.value?.id] : props.organization.segments,
    },
    true,
  ).then(({ count }) => {
    totalContacts.value = count;
  });
};

const fetch = () => {
  loading.value = true;
  MemberService.listMembers({
    filter: {
      and: [orgFilter, savedBody.value],
    },
    search: searchStr.value,
    offset: (pagination.value.page - 1) * pagination.value.perPage,
    limit: pagination.value.perPage,
    orderBy: sort.value,
    segments: selectedProjectGroup.value?.id ? [selectedProjectGroup.value?.id] : props.organization.segments,
  })
    .then((data: Pagination<Member>) => {
      if (pagination.value.page > 1) {
        contributors.value = [...contributors.value, ...data.rows];
      } else {
        contributors.value = data.rows;
      }
      pagination.value.total = data.count;
    })
    .catch((err) => {
      contributors.value = [];
      pagination.value.total = 0;
    })
    .finally(() => {
      loading.value = false;
    });
};

const loadMore = () => {
  if (pagination.value.total <= pagination.value.page * pagination.value.perPage) {
    return;
  }
  pagination.value.page += 1;
  fetch();
};

const onFilterChange = (filterQuery: FilterQuery) => {
  savedBody.value = filterQuery.body;
  searchStr.value = filterQuery.search;
  pagination.value.page = 1;
  pagination.value.total = 0;
  fetch();
};

const onSortChange = (sortingValue: string) => {
  sort.value = sortingValue;
  pagination.value.page = 1;
  fetch();
};

onMounted(() => {
  doGetMembersCount();
  fetch();
});

// eslint-disable-next-line vue/define-macros-order -- the plain <script> block below counts as last
defineExpose({
  loadMore,
});
</script>

<script lang="ts">
export default {
  name: 'LfOrganizationDetailsContributors',
};
</script>
