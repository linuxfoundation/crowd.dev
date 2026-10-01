<template>
  <div>
    <div class="pt-3">
      <div
        v-if="isLoading"
        class="h-16 !relative !min-h-5 flex justify-center items-center"
      >
        <div class="animate-spin w-fit">
          <div class="custom-spinner" />
        </div>
      </div>
      <div v-else>
        <!-- Empty State -->
        <app-empty-state-cta
          v-if="!hasOrganizations"
          icon="house-building"
          title="No organizations yet"
          :description="`We couldn't track any organizations related to all the people who interacted with ${selectedProjectGroup.name} projects.`"
          cta-btn="Add organization"
          @cta-click="onCtaClick"
        />

        <app-empty-state-cta
          v-else-if="hasOrganizations && !totalOrganizations"
          icon="house-building"
          title="No organizations found"
          description="We couldn't find any results that match your search criteria, please try a different query."
        />

        <div v-else>
          <!-- Sorter -->
          <div class="mb-2">
            <app-pagination-sorter
              :page-size="Number(pagination.perPage)"
              :total="totalOrganizations"
              :current-page="pagination.page"
              :has-page-counter="false"
              :export="doExport"
              module="organization"
              position="top"
              @change-sorter="doChangePaginationPageSize"
            >
              <template #defaultFilters>
                <div>・</div>
                <lf-default-filters
                  :config="organizationSavedViews"
                  :settings="filters.settings"
                />
              </template>
            </app-pagination-sorter>
          </div>

          <!-- Organizations list -->
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

            <app-organization-list-toolbar
              :pagination="pagination"
              @mouseover="onTableMouseover"
              @mouseleave="onTableMouseLeft"
            />

            <div
              class="-mx-6 -mt-6 relative"
              @mouseover="onTableMouseover"
              @mouseleave="onTableMouseLeft"
            >
              <lf-table
                id="organizations-table"
                ref="table"
                type="bordered"
                show-hover
              >
                <thead>
                  <tr>
                    <lf-table-head class="!py-4 min-w-19" sticky>
                      <lf-checkbox
                        class="!m-0"
                        :model-value="
                          selectedOrganizations.length === organizations.length
                        "
                        :indeterminate="
                          selectedOrganizations.length > 0
                            && selectedOrganizations.length < organizations.length
                        "
                        @update:model-value="toggleAllOrganizationsSelection()"
                      />
                    </lf-table-head>
                    <lf-table-head
                      property="displayName"
                      :model-value="sorting"
                      class="!py-4 px-3 min-w-76 !left-19"
                      sticky
                      @update:model-value="doChangeSort($event)"
                    >
                      Organization
                    </lf-table-head>
                    <lf-table-head class="!py-4 !px-3 min-w-66">
                      Website
                    </lf-table-head>

                    <lf-table-head class="!py-4 !px-3 min-w-76">
                      <el-tooltip placement="top">
                        <template #content>
                          Identities can be profiles on social platforms,
                          emails,<br />
                          or unique identifiers from internal sources.
                        </template>
                        <span
                          class="underline decoration-dashed decoration-gray-400 underline-offset-4"
                        >Identities</span>
                      </el-tooltip>
                    </lf-table-head>

                    <lf-table-head
                      class="!py-4 !px-3 min-w-55"
                      property="memberCount"
                      :model-value="sorting"
                      @update:model-value="doChangeSort($event)"
                    >
                      # of People
                    </lf-table-head>

                    <lf-table-head
                      class="!py-4 !px-3 min-w-55"
                      property="activityCount"
                      :model-value="sorting"
                      @update:model-value="doChangeSort($event)"
                    >
                      # of Activities
                    </lf-table-head>

                    <lf-table-head v-if="showReach" class="!py-4 !px-3 min-w-55">
                      <div
                        :ref="
                          (el) => setEnrichmentAttributesRef(el, 'revenueRange')
                        "
                        class="flex items-center"
                        @mouseover="
                          () => onColumnHeaderMouseOver('revenueRange')
                        "
                        @mouseleave="closeEnrichmentPopover"
                      >
                        <el-tooltip
                          content="Source: Enrichment"
                          placement="top"
                          trigger="hover"
                        >
                          <lf-svg name="source" class="h-3 w-3" />
                        </el-tooltip>
                        <div class="ml-2 text-purple-800">
                          Annual Revenue
                        </div>
                      </div>
                    </lf-table-head>

                    <lf-table-head
                      v-if="hasPermissions"
                      sticky
                      class="!py-4 min-w-19"
                    />
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="org of organizations"
                    :key="org.id"
                    :class="isSelected(org) ? 'is-selected' : ''"
                    :data-qa="`organization-${org.id}`"
                  >
                    <lf-table-cell sticky class="!py-4">
                      <lf-checkbox
                        class="!m-0"
                        :model-value="isSelected(org)"
                        @update:model-value="toggleOrgSelection(org)"
                      />
                    </lf-table-cell>

                    <!-- Organization logo and name -->
                    <lf-table-cell sticky class="!py-4 pl-2 !left-19">
                      <router-link
                        :to="{
                          name: 'organizationView',
                          params: { id: org.id },
                          query: {
                            projectGroup: selectedProjectGroup?.id,
                            segmentId: org.segmentId,
                          },
                        }"
                        class="block mr-4"
                      >
                        <app-organization-name
                          class="w-full"
                          :organization="org"
                        />
                      </router-link>
                    </lf-table-cell>

                    <!-- Website -->
                    <lf-table-cell class="!py-4 pl-3">
                      <router-link
                        :to="{
                          name: 'organizationView',
                          params: { id: org.id },
                        }"
                        class="block"
                      >
                        <div class="text-sm h-full flex items-center">
                          <a
                            v-if="getOrganizationWebsite(org)"
                            class="website-link hover:!text-gray-900"
                            :href="withHttp(getOrganizationWebsite(org))"
                            target="_blank"
                            rel="noopener noreferrer"
                            @click.stop
                          >{{ getOrganizationWebsite(org) }}</a>
                          <span v-else class="text-gray-500">-</span>
                        </div>
                      </router-link>
                    </lf-table-cell>

                    <!-- Identities -->
                    <lf-table-cell class="!py-4 pl-3">
                      <router-link
                        :to="{
                          name: 'organizationView',
                          params: { id: org.id },
                          query: {
                            projectGroup: selectedProjectGroup?.id,
                            segmentId: org.segmentId,
                          },
                        }"
                        class="block"
                      >
                        <div class="h-full flex items-center">
                          <app-identities-horizontal-list-organizations
                            :organization="org"
                            :limit="5"
                          />
                        </div>
                      </router-link>
                    </lf-table-cell>

                    <!-- Number of members -->
                    <lf-table-cell class="!py-4 pl-3">
                      <router-link
                        :to="{
                          name: 'organizationView',
                          params: { id: org.id },
                          query: {
                            projectGroup: selectedProjectGroup?.id,
                            segmentId: org.segmentId,
                          },
                        }"
                        class="block"
                      >
                        <div
                          class="text-gray-900 text-sm h-full flex items-center"
                        >
                          {{ formatNumberToCompact(org.memberCount) }}
                        </div>
                      </router-link>
                    </lf-table-cell>

                    <!-- Number of activities -->
                    <lf-table-cell class="!py-4 pl-3">
                      <router-link
                        :to="{
                          name: 'organizationView',
                          params: { id: org.id },
                          query: {
                            projectGroup: selectedProjectGroup?.id,
                            segmentId: org.segmentId,
                          },
                        }"
                        class="block"
                      >
                        <div
                          class="text-gray-900 text-sm h-full flex items-center"
                        >
                          {{ formatNumberToCompact(org.activityCount) }}
                        </div>
                      </router-link>
                    </lf-table-cell>

                    <!-- Inferred Revenue -->
                    <lf-table-cell
                      v-if="showReach"
                      class="!py-4 pl-3"
                      @mouseover="
                        () => handleCellMouseEnter(org, 'revenueRange')
                      "
                      @mouseleave="closeEnrichmentPopover"
                    >
                      <router-link
                        :ref="
                          (el) =>
                            setEnrichmentAttributesRef(
                              el,
                              `${org.id}-revenueRange`,
                            )
                        "
                        :to="{
                          name: 'organizationView',
                          params: { id: org.id },
                          query: {
                            projectGroup: selectedProjectGroup?.id,
                            segmentId: org.segmentId,
                          },
                        }"
                        class="block h-full"
                      >
                        <div class="text-sm h-full flex items-center">
                          <span v-if="org.revenueRange" class="text-gray-900">
                            {{ revenueRange.formatValue(org.revenueRange) }}
                          </span>
                          <span v-else class="text-gray-500">-</span>
                        </div>
                      </router-link>
                    </lf-table-cell>

                    <!-- Action button -->
                    <lf-table-cell
                      v-if="hasPermissions"
                      sticky
                      class="!py-4 pr-2"
                    >
                      <router-link
                        :to="{
                          name: 'organizationView',
                          params: { id: org.id },
                          query: {
                            projectGroup: selectedProjectGroup?.id,
                            segmentId: org.segmentId,
                          },
                        }"
                        class="flex justify-center"
                      >
                        <button
                          :id="`buttonRef-${org.id}`"
                          :ref="(el) => setActionBtnsRef(el, org.id)"
                          class="el-dropdown-link btn p-1.5 rounder-md hover:bg-gray-200 text-gray-600"
                          type="button"
                          @click.prevent.stop="() => onActionBtnClick(org)"
                        >
                          <lf-icon
                            :id="`buttonRefIcon-${org.id}`"
                            name="ellipsis"
                            type="regular"
                            :size="24"
                          />
                        </button>
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

              <div v-if="showBottomPagination" class="mt-8 px-6">
                <app-pagination
                  :total="totalOrganizations"
                  :page-size="Number(pagination.perPage)"
                  :current-page="pagination.page || 1"
                  module="organization"
                  @change-current-page="doChangePaginationCurrentPage"
                  @change-page-size="doChangePaginationPageSize"
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <!-- Actions dropdown popover -->
    <el-popover
      placement="bottom-end"
      popper-class="popover-dropdown"
      :virtual-ref="actionBtnRefs[selectedActionOrganization?.id]"
      trigger="click"
      :visible="showOrganizationDropdownPopover"
      virtual-triggering
    >
      <div v-click-outside="onClickOutside">
        <app-organization-dropdown-content
          v-if="selectedActionOrganization"
          :organization="selectedActionOrganization"
          hide-unmerge
          @merge="isMergeDialogOpen = selectedActionOrganization"
          @close-dropdown="closeDropdown"
        />
      </div>
    </el-popover>

    <app-organization-merge-dialog v-model="isMergeDialogOpen" />
  </div>
</template>

<script setup>
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import AppOrganizationMergeDialog from '@/modules/organization/components/organization-merge-dialog.vue';
import revenueRange from '@/modules/organization/config/enrichment/revenueRange';
import { OrganizationService } from '@/modules/organization/organization-service';
import { useOrganizationStore } from '@/modules/organization/store/pinia';
import AppEmptyStateCta from '@/shared/empty-state/empty-state-cta.vue';
import LfDefaultFilters from '@/shared/modules/default-filters/components/default-filters.vue';
import AppIdentitiesHorizontalListOrganizations from '@/shared/modules/identities/components/identities-horizontal-list-organizations.vue';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import LfSvg from '@/shared/svg/svg.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableCell from '@/ui-kit/table/TableCell.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import { formatNumberToCompact } from '@/utils/number';
import { getOrganizationWebsite } from '@/utils/organization';
import { withHttp } from '@/utils/string';
import { ClickOutside as vClickOutside } from 'element-plus';
import { storeToRefs } from 'pinia';
import {
  computed,
  onUnmounted,
  ref, watch,
} from 'vue';
import { useRouter } from 'vue-router';
import { organizationSavedViews } from '../../config/saved-views/main';
import AppOrganizationDropdownContent from '../organization-dropdown-content.vue';
import AppOrganizationName from '../organization-name.vue';
import AppOrganizationListToolbar from './organization-list-toolbar.vue';

const props = defineProps({
  hasOrganizations: {
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
const emit = defineEmits(['update:pagination']);
const { trackEvent } = useProductTracking();
const router = useRouter();

const { hasPermission } = usePermissions();

const hasPermissions = computed(() => [
  LfPermission.organizationEdit,
  LfPermission.organizationDestroy,
  LfPermission.mergeOrganizations,
].some((permission) => hasPermission(permission)));

const organizationStore = useOrganizationStore();
const {
  organizations,
  selectedOrganizations,
  filters,
  totalOrganizations,
  savedFilterBody,
} = storeToRefs(organizationStore);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const isMergeDialogOpen = ref(null);
const table = ref(null);
const scrollbarRef = ref();
const tableBodyRef = ref();
const tableHeaderRef = ref();
const isScrollbarVisible = ref(false);
const isTableHovered = ref(false);
const isCursorDown = ref(false);

const showOrganizationDropdownPopover = ref(false);
const actionBtnRefs = ref({});
const selectedActionOrganization = ref(null);
const sorting = computed(
  () => `${filters.value.order.prop}_${filters.value.order.order === 'descending' ? 'DESC' : 'ASC'}`,
);

const showEnrichmentPopover = ref(false);
const enrichmentRefs = ref({});
const selectedEnrichmentAttribute = ref(null);

const pagination = computed({
  get() {
    return props.pagination;
  },
  set(value) {
    emit('update:pagination', value);
  },
});

const showBottomPagination = computed(
  () => !!totalOrganizations.value
    && Math.ceil(totalOrganizations.value / Number(pagination.value.perPage)) > 1,
);
const isLoading = computed(() => props.isPageLoading);

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

const onActionBtnClick = (organization) => {
  if (selectedActionOrganization.value?.id === organization.id) {
    showOrganizationDropdownPopover.value = false;

    setTimeout(() => {
      selectedActionOrganization.value = null;
    }, 200);
  } else {
    showOrganizationDropdownPopover.value = true;
    selectedActionOrganization.value = organization;
  }
};

const setEnrichmentAttributesRef = (el, id) => {
  if (el) {
    enrichmentRefs.value[id] = el;
  }
};

const handleCellMouseEnter = (row, columnName) => {
  showEnrichmentPopover.value = true;
  selectedEnrichmentAttribute.value = `${row.id}-${columnName}`;
};

const onColumnHeaderMouseOver = (id) => {
  showEnrichmentPopover.value = true;
  selectedEnrichmentAttribute.value = id;
};

const closeEnrichmentPopover = (ev) => {
  if (ev?.toElement?.id !== 'popover-content') {
    showEnrichmentPopover.value = false;

    setTimeout(() => {
      selectedEnrichmentAttribute.value = null;
    }, 100);
  }
};

const closeDropdown = () => {
  showOrganizationDropdownPopover.value = false;

  setTimeout(() => {
    selectedActionOrganization.value = null;
  }, 200);
};

const onClickOutside = (el) => {
  if (!el.target?.id.includes('buttonRef')) {
    closeDropdown();
  }
};

const toggleAllOrganizationsSelection = () => {
  if (selectedOrganizations.value.length === organizations.value.length) {
    selectedOrganizations.value = [];
  } else {
    selectedOrganizations.value = organizations.value;
  }
};

const toggleOrgSelection = (org) => {
  if (isSelected(org)) {
    selectedOrganizations.value = selectedOrganizations.value.filter(
      (r) => r.id !== org.id,
    );
  } else {
    selectedOrganizations.value.push(org);
  }
};

const isSelected = (org) => selectedOrganizations.value.find((r) => r.id === org.id) !== undefined;

function doChangeSort(sorter) {
  trackEvent({
    key: FeatureEventKey.SORT_ORGANIZATIONS,
    type: EventType.FEATURE,
    properties: {
      orderby: sorter,
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

const onCtaClick = () => {
  router.push({
    name: 'organizationCreate',
  });
};

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

const doExport = () => OrganizationService.export({
  filter: savedFilterBody.value.filter,
  orderBy: savedFilterBody.value.orderBy,
  limit: totalOrganizations.value,
  offset: null,
  segments: [selectedProjectGroup.value?.id],
});

watch(table, (newValue) => {
  // Add scroll events to table, it's not possible to access it from 'el-table'
  // as the overflowed element is within it
  const tableBodyEl = document.querySelector(
    '#organizations-table .el-scrollbar__wrap',
  );
  const tableHeaderEl = document.querySelector(
    '#organizations-table .el-table__header-wrapper',
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

// Remove listeners on unmount
onUnmounted(() => {
  tableBodyRef.value?.removeEventListener('scroll', onTableBodyScroll);
  tableHeaderRef.value?.removeEventListener('scroll', onTableHeaderScroll);
});
</script>

<script>
export default {
  name: 'AppOrganizationListTable',
};
</script>

<style lang="scss">
.website-link {
  @apply text-gray-900 text-sm line-clamp-1 font-medium underline decoration-dashed
  decoration-gray-400 underline-offset-4 hover:decoration-gray-900 hover:cursor-pointer;
}

.popover-dropdown {
  padding: 0.5rem !important;
  width: fit-content !important;
}
</style>
