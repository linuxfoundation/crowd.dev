<template>
  <div>
    <div
      v-if="loading"
      class="h-16 !relative !min-h-5 flex justify-center items-center"
    >
      <div class="animate-spin w-fit">
        <div class="custom-spinner" />
      </div>
    </div>
    <div v-else>
      <!-- Empty State -->
      <app-empty-state-cta
        v-if="!hasIntegrations && !hasMembers"
        icon="user-group-simple"
        title="No people in your community yet"
        description="Please connect with one of our available data sources in order to start pulling data from a certain platform"
        secondary-btn="Add person"
        @secondary-click="onSecondaryBtnClick"
      />

      <app-empty-state-cta
        v-else-if="hasIntegrations && !hasMembers"
        icon="user-group-simple"
        title="No people in your community yet"
        description="Please consider that the first people may take a couple of minutes to be displayed"
        :has-warning-icon="true"
      />

      <app-empty-state-cta
        v-else-if="hasMembers && !totalMembers"
        icon="user-group-simple"
        title="No people found"
        description="We couldn't find any results that match your search criteria, please try a different query"
      />

      <div v-else>
        <!-- Sorter -->
        <div class="mb-2">
          <app-pagination-sorter
            :page-size="Number(pagination.perPage)"
            :total="totalMembers"
            :current-page="pagination.page"
            :has-page-counter="false"
            :export="doExport"
            module="member"
            position="top"
            @change-sorter="doChangePaginationPageSize"
          >
            <template #defaultFilters>
              <div>・</div>
              <lf-default-filters
                :config="memberSavedViews"
                :settings="filters.settings"
              />
            </template>
          </app-pagination-sorter>
        </div>

        <!-- Members list -->
        <div class="relative panel">
          <transition name="el-fade-in">
            <div
              v-show="isScrollbarVisible"
              class="absolute z-20 top-0 left-0 w-full"
              @mouseover="onTableMouseover"
              @mouseleave="onTableMouseLeft"
            >
              <el-scrollbar
                id="custom-scrollbar"
                ref="scrollbarRef"
                height="10px"
                always
                @scroll="onCustomScrollbarScroll"
                @pointerdown="onScrollMousedown"
              >
                <div
                  :style="{
                    height: '10px',
                  }"
                />
              </el-scrollbar>
            </div>
          </transition>
          <app-member-list-toolbar
            @mouseover="onTableMouseover"
            @mouseleave="onTableMouseLeft"
          />
          <div
            class="-mx-6 -mt-6 relative"
            @mouseover="onTableMouseover"
            @mouseleave="onTableMouseLeft"
          >
            <lf-table
              id="members-table"
              ref="table"
              v-loading="loading"
              type="bordered"
              show-hover
            >
              <thead>
                <tr>
                  <lf-table-head class="!py-4 px-2 min-w-19" :sticky="true">
                    <lf-checkbox
                      class="!m-0"
                      :model-value="selectedRows.length === members.length"
                      :indeterminate="
                        selectedRows.length > 0
                          && selectedRows.length < members.length
                      "
                      @update:model-value="toggleAllMembersSelection()"
                    />
                  </lf-table-head>
                  <lf-table-head
                    property="displayName"
                    :model-value="sorting"
                    class="!py-4 !px-3 min-w-76 !left-19"
                    :sticky="true"
                    @update:model-value="doChangeSort($event)"
                  >
                    Person
                  </lf-table-head>
                  <lf-table-head class="!py-4 !px-3 min-w-76">
                    <div class="flex items-center">
                      <el-tooltip
                        content="Source: Enrichment & GitHub"
                        placement="top"
                        trigger="hover"
                      >
                        <lf-svg name="source" class="h-3 w-3" />
                      </el-tooltip>
                      <div class="ml-2 text-purple-800">
                        Organization
                      </div>
                    </div>
                  </lf-table-head>

                  <lf-table-head class="!py-4 !px-3 min-w-66">
                    <div class="flex items-center">
                      <el-tooltip
                        content="Source: Enrichment & GitHub"
                        placement="top"
                        trigger="hover"
                      >
                        <lf-svg name="source" class="h-3 w-3" />
                      </el-tooltip>
                      <div class="ml-2 text-purple-800">
                        Job Title
                      </div>
                    </div>
                  </lf-table-head>

                  <lf-table-head class="!py-4 !px-3 min-w-76">
                    <el-tooltip placement="top">
                      <template #content>
                        Identities can be profiles on social platforms, emails,
                        phone numbers,<br />
                        or unique identifiers from internal sources (e.g. web
                        app log-in email).
                      </template>
                      <span
                        class="underline decoration-dashed decoration-gray-400 underline-offset-4"
                      >Identities</span>
                    </el-tooltip>
                  </lf-table-head>

                  <lf-table-head class="!py-4 !px-3 min-w-76">
                    Emails
                  </lf-table-head>

                  <lf-table-head
                    class="!py-4 !px-3 min-w-55"
                    property="activityCount"
                    :model-value="sorting"
                    @update:model-value="doChangeSort($event)"
                  >
                    # of Activities
                  </lf-table-head>

                  <lf-table-head
                    v-if="hasPermissions"
                    :sticky="true"
                    class="!py-4 min-w-19"
                  />
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="member of members"
                  :key="member.id"
                  :class="isSelected(member) ? 'is-selected' : ''"
                  :data-qa="`member-${member.id}`"
                >
                  <lf-table-cell :sticky="true" class="!py-4 pl-2">
                    <lf-checkbox
                      class="!m-0"
                      :model-value="isSelected(member)"
                      @update:model-value="toggleMemberSelection(member)"
                    />
                  </lf-table-cell>

                  <!-- Contacts -->
                  <lf-table-cell :sticky="true" class="!py-4 pl-2 !left-19">
                    <router-link
                      :to="{
                        name: 'memberView',
                        params: { id: member.id },
                        query: { projectGroup: selectedProjectGroup?.id },
                      }"
                      class="block"
                    >
                      <div class="flex items-center">
                        <div
                          class="inline-flex flex-wrap overflow-wrap items-center"
                        >
                          <app-avatar :entity="member" size="xs" class="mr-3" />

                          <span
                            class="font-medium text-sm text-gray-900 line-clamp-2 w-auto"
                            data-qa="members-name"
                            v-html="$sanitize(member.displayName)"
                          />
                          <app-member-sentiment
                            :member="member"
                            class="ml-1 mr-1"
                          />
                          <app-member-badge :member="member" />
                        </div>
                      </div>
                    </router-link>
                  </lf-table-cell>

                  <!-- Organization -->
                  <lf-table-cell class="!py-4 pl-3">
                    <router-link
                      :to="{
                        name: 'memberView',
                        params: { id: member.id },
                        query: { projectGroup: selectedProjectGroup?.id },
                      }"
                      class="block"
                    >
                      <app-member-organizations-vertical :member="member" />
                    </router-link>
                  </lf-table-cell>

                  <!-- Job Title -->
                  <lf-table-cell class="!py-4 pl-3">
                    <router-link
                      :to="{
                        name: 'memberView',
                        params: { id: member.id },
                        query: { projectGroup: selectedProjectGroup?.id },
                      }"
                      class="block"
                    >
                      <app-member-job-title :member="member" />
                    </router-link>
                  </lf-table-cell>

                  <!-- Identities -->
                  <lf-table-cell class="!py-4 pl-3">
                    <router-link
                      :to="{
                        name: 'memberView',
                        params: { id: member.id },
                        query: { projectGroup: selectedProjectGroup?.id },
                      }"
                      class="block w-fit"
                    >
                      <app-identities-horizontal-list-members
                        :member="member"
                        :limit="5"
                      />
                    </router-link>
                  </lf-table-cell>

                  <!-- Emails -->
                  <lf-table-cell class="!py-4 pl-3">
                    <router-link
                      :to="{
                        name: 'memberView',
                        params: { id: member.id },
                        query: { projectGroup: selectedProjectGroup?.id },
                      }"
                      class="block"
                    >
                      <app-member-list-emails :member="member" />
                    </router-link>
                  </lf-table-cell>

                  <!-- # of Activities -->
                  <lf-table-cell class="!py-4 pl-3">
                    <router-link
                      :to="{
                        name: 'memberView',
                        params: { id: member.id },
                        query: { projectGroup: selectedProjectGroup?.id },
                      }"
                      class="block !text-gray-500"
                    >
                      {{ formatNumber(member.activityCount) }}
                    </router-link>
                  </lf-table-cell>

                  <!-- Action button -->
                  <lf-table-cell
                    v-if="hasPermissions"
                    :sticky="true"
                    class="!py-4 pr-2"
                  >
                    <router-link
                      :to="{
                        name: 'memberView',
                        params: { id: member.id },
                        query: { projectGroup: selectedProjectGroup?.id },
                      }"
                      class="block w-full"
                    >
                      <div
                        class="h-full flex items-center justify-center w-full"
                      >
                        <button
                          :id="`buttonRef-${member.id}`"
                          :ref="(el) => setActionBtnsRef(el, member.id)"
                          class="el-dropdown-link btn p-1.5 rounder-md hover:bg-gray-200 text-gray-600"
                          type="button"
                          @click.prevent.stop="() => onActionBtnClick(member)"
                        >
                          <lf-icon
                            :id="`buttonRefIcon-${member.id}`"
                            name="ellipsis"
                            type="solid"
                            :size="24"
                          />
                        </button>
                      </div>
                    </router-link>
                  </lf-table-cell>
                </tr>
              </tbody>
            </lf-table>
            <div
              v-if="isTableLoading"
              class="absolute w-full top-0 left-0 bottom-[64px] bg-white opacity-60 z-20 flex items-center justify-center"
            >
              <div
                class="h-16 !relative !min-h-5 flex justify-center items-center"
              >
                <div class="animate-spin w-fit">
                  <div class="custom-spinner" />
                </div>
              </div>
            </div>

            <div v-if="!!totalMembers" class="mt-8 px-6">
              <app-pagination
                :total="totalMembers"
                :page-size="Number(pagination.perPage)"
                :current-page="pagination.page || 1"
                module="member"
                @change-current-page="doChangePaginationCurrentPage"
                @change-page-size="doChangePaginationPageSize"
              />
            </div>
          </div>
        </div>
      </div>
    </div>
    <!-- Actions dropdown popover -->
    <el-popover
      placement="bottom-end"
      popper-class="popover-dropdown"
      :virtual-ref="actionBtnRefs[selectedActionMember?.id]"
      trigger="click"
      :visible="showMemberDropdownPopover"
      virtual-triggering
    >
      <div v-click-outside="onClickOutside">
        <app-member-dropdown-content
          v-if="selectedActionMember"
          :member="selectedActionMember"
          :hide-unmerge="true"
          @find-github="isFindGithubDrawerOpen = selectedActionMember"
          @merge="isMergeDialogOpen = selectedActionMember"
          @close-dropdown="closeDropdown"
        />
      </div>
    </el-popover>

    <app-member-find-github-drawer
      v-if="isFindGithubDrawerOpen"
      v-model="isFindGithubDrawerOpen"
    />
    <app-member-merge-dialog v-model="isMergeDialogOpen" />
  </div>
</template>

<script setup>
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import AppMemberListEmails from '@/modules/member/components/list/columns/member-list-emails.vue';
import AppMemberListToolbar from '@/modules/member/components/list/member-list-toolbar.vue';
import AppMemberFindGithubDrawer from '@/modules/member/components/member-find-github-drawer.vue';
import AppMemberJobTitle from '@/modules/member/components/member-job-title.vue';
import AppMemberMergeDialog from '@/modules/member/components/member-merge-dialog.vue';
import AppMemberOrganizationsVertical from '@/modules/member/components/member-organizations-vertical.vue';
import { MemberService } from '@/modules/member/member-service';
import { useMemberStore } from '@/modules/member/store/pinia';
import AppEmptyStateCta from '@/shared/empty-state/empty-state-cta.vue';
import LfDefaultFilters from '@/shared/modules/default-filters/components/default-filters.vue';
import AppIdentitiesHorizontalListMembers from '@/shared/modules/identities/components/identities-horizontal-list-members.vue';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import AppPagination from '@/shared/pagination/pagination.vue';
import LfSvg from '@/shared/svg/svg.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableCell from '@/ui-kit/table/TableCell.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import { formatNumber } from '@/utils/number';
import { getSegmentsFromProjectGroup } from '@/utils/segments';
import { ClickOutside as vClickOutside } from 'element-plus';
import { storeToRefs } from 'pinia';
import {
  computed,
  defineProps,
  onMounted, onUnmounted, ref,
  watch,
} from 'vue';
import { useStore } from 'vuex';
import { memberSavedViews } from '../../config/saved-views/main';
import AppMemberBadge from '../member-badge.vue';
import AppMemberDropdownContent from '../member-dropdown-content.vue';
import AppMemberSentiment from '../member-sentiment.vue';

const { trackEvent } = useProductTracking();
const store = useStore();
const table = ref(null);
const scrollbarRef = ref();
const tableBodyRef = ref();
const tableHeaderRef = ref();
const isScrollbarVisible = ref(false);
const isTableHovered = ref(false);
const isCursorDown = ref(false);

const isMergeDialogOpen = ref(null);

const showMemberDropdownPopover = ref(false);
const actionBtnRefs = ref({});
const selectedActionMember = ref(null);

const isFindGithubDrawerOpen = ref(null);

const props = defineProps({
  hasIntegrations: {
    type: Boolean,
    default: () => false,
  },
  hasMembers: {
    type: Boolean,
    default: () => false,
  },
  isPageLoading: {
    type: Boolean,
    default: () => true,
  },
  isTableLoading: {
    type: Boolean,
    default: () => false,
  },
  pagination: {
    type: Object,
    default: () => ({
      page: 1,
      perPage: 20,
    }),
  },
});

const emit = defineEmits(['onAddMember', 'update:pagination']);

const memberStore = useMemberStore();
const {
  members, totalMembers, filters, selectedMembers, savedFilterBody,
} = storeToRefs(memberStore);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const { hasPermission } = usePermissions();

const hasPermissions = computed(() => [
  LfPermission.memberEdit,
  LfPermission.memberDestroy,
  LfPermission.mergeMembers,
].some((permission) => hasPermission(permission)));

const sorting = computed(
  () => `${filters.value.order.prop}_${filters.value.order.order === 'descending' ? 'DESC' : 'ASC'}`,
);

const loading = computed(() => props.isPageLoading);

const selectedRows = computed(() => selectedMembers.value);
const pagination = computed({
  get() {
    return props.pagination;
  },
  set(value) {
    emit('update:pagination', value);
  },
});

const tableWidth = ref(0);

document.onmouseup = () => {
  // As soon as mouse is released, set scrollbar visibility
  // according to wether the mouse is hovering the table or not
  isScrollbarVisible.value = isTableHovered.value;
  isCursorDown.value = false;
};

const setActionBtnsRef = (el, id) => {
  if (el) {
    actionBtnRefs.value[id] = el;
  }
};

const onActionBtnClick = (member) => {
  if (selectedActionMember.value?.id === member.id) {
    showMemberDropdownPopover.value = false;

    setTimeout(() => {
      selectedActionMember.value = null;
    }, 200);
  } else {
    showMemberDropdownPopover.value = true;
    selectedActionMember.value = member;
  }
};

const closeDropdown = () => {
  showMemberDropdownPopover.value = false;

  setTimeout(() => {
    selectedActionMember.value = null;
  }, 200);
};

const onClickOutside = (el) => {
  if (!el.target?.id.includes('buttonRef')) {
    closeDropdown();
  }
};

const toggleAllMembersSelection = () => {
  if (selectedRows.value.length === members.value.length) {
    selectedMembers.value = [];
  } else {
    selectedMembers.value = members.value;
  }
};

const toggleMemberSelection = (member) => {
  if (isSelected(member)) {
    selectedMembers.value = selectedMembers.value.filter(
      (r) => r.id !== member.id,
    );
  } else {
    selectedMembers.value.push(member);
  }
};

const isSelected = (member) => selectedMembers.value.find((r) => r.id === member.id) !== undefined;

function doChangeSort(sorter) {
  trackEvent({
    key: FeatureEventKey.SORT_MEMBERS,
    type: EventType.FEATURE,
    properties: {
      orderBy: sorter,
    },
  });
  const orderby = sorter.split('_');
  filters.value.order = {
    prop: orderby[0],
    order: orderby[1] === 'DESC' ? 'descending' : 'ascending',
  };
}

function doChangePaginationCurrentPage(currentPage) {
  emit('update:pagination', {
    ...pagination.value,
    page: currentPage,
  });
}

function doChangePaginationPageSize(pageSize) {
  emit('update:pagination', {
    page: 1,
    perPage: pageSize,
  });
}

function onSecondaryBtnClick() {
  emit('onAddMember');
}

// On custom scrollbar scroll, set the table scroll with the same value
const onCustomScrollbarScroll = ({ scrollLeft }) => {
  table.value.setScrollLeft(scrollLeft);
};

// On table body scroll, set the custom scrollbar scroll with the same value
const onTableBodyScroll = () => {
  scrollbarRef.value.setScrollLeft(tableBodyRef.value.scrollLeft);
};

// On table header scroll, set the custom scrollbar scroll with the same value
const onTableHeaderScroll = () => {
  scrollbarRef.value.setScrollLeft(tableHeaderRef.value.scrollLeft);
  table.value.setScrollLeft(tableHeaderRef.value.scrollLeft);
};

const onScrollMousedown = () => {
  isCursorDown.value = true;
};

const onTableMouseover = () => {
  isTableHovered.value = true;
  isScrollbarVisible.value = true;
};

const onTableMouseLeft = () => {
  isTableHovered.value = false;
  isScrollbarVisible.value = isCursorDown.value;
};

watch(table, (newValue) => {
  if (newValue) {
    tableWidth.value = table.value;
  }

  // Add scroll events to table, it's not possible to access it from 'el-table'
  // as the overflowed element is within it
  const tableBodyEl = document.querySelector(
    '#members-table .el-scrollbar__wrap',
  );
  const tableHeaderEl = document.querySelector(
    '#members-table .el-table__header-wrapper',
  );

  if (tableBodyEl) {
    tableBodyRef.value = tableBodyEl;
    tableBodyRef.value.addEventListener('scroll', onTableBodyScroll);
  }

  if (tableHeaderEl) {
    tableHeaderEl.style.overflow = 'auto';
    tableHeaderRef.value = tableHeaderEl;
    tableHeaderRef.value.addEventListener('scroll', onTableHeaderScroll);
  }
});

const doExport = () => MemberService.export({
  filter: savedFilterBody.value.filter,
  orderBy: savedFilterBody.value.orderBy,
  limit: totalMembers.value,
  offset: null,
  segments: [selectedProjectGroup.value?.id],
});

onMounted(async () => {
  await store.dispatch(
    'integration/doFetch',
    getSegmentsFromProjectGroup(selectedProjectGroup.value),
  );
});

// Remove listeners on unmount
onUnmounted(() => {
  tableBodyRef.value?.removeEventListener('scroll', onTableBodyScroll);
  tableHeaderRef.value?.removeEventListener('scroll', onTableHeaderScroll);
});
</script>

<script>
export default {
  name: 'AppMemberListTable',
};
</script>

<style lang="scss">
.popover-dropdown {
  @apply p-2 w-fit #{!important};
}
</style>
