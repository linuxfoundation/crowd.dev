<template>
  <div class="pt-6">
    <lf-filter
      ref="memberFilter"
      v-model="filters"
      :config="auditLogsFilters"
      :lock-relation="true"
      hash="audit-logs"
      class="flex flex-row-reverse justify-between"
      @fetch="onFilterChange($event)"
    />
  </div>
  <div
    v-if="loading"
    class="h-16 !relative !min-h-5 flex justify-center items-center"
  >
    <div class="animate-spin w-fit">
      <div class="custom-spinner" />
    </div>
  </div>
  <div v-else>
    <lf-table
      id="audit-logs-table"
      ref="table"
      v-loading="loading"
      type="bordered"
      class="!overflow-visible mt-4 cursor-pointer"
      show-hover
    >
      <thead>
        <tr>
          <lf-table-head class="pl-2">
            Action
          </lf-table-head>
          <lf-table-head class="pl-3">
            Actor
          </lf-table-head>
          <lf-table-head class="pl-3 w-52">
            Timestamp
          </lf-table-head>
          <lf-table-head class="w-34" />
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="auditLog of auditLogs"
          :key="auditLog.id"
          @click="openLogDetails = auditLog"
        >
          <lf-table-cell class="pl-2">
            <div class="flex">
              <div class="pr-2 min-w-6">
                <lf-icon
                  v-if="auditLog.success"
                  type="solid"
                  name="circle-check"
                  :size="16"
                  class="text-green-500 mr-1"
                />
                <lf-icon
                  v-else
                  type="solid"
                  name="circle-exclamation"
                  :size="16"
                  class="text-red-500 mr-1"
                />
              </div>
              <div>
                <div class="text-sm font-semibold text-black mb-1 leading-5">
                  {{
                    logRenderingConfig[auditLog.actionType]?.label
                      ?? auditLog.actionType
                  }}
                </div>
                <p
                  v-if="logRenderingConfig[auditLog.actionType]"
                  class="text-2xs text-gray-500 leading-5"
                  v-html="
                    $sanitize(
                      logRenderingConfig[auditLog.actionType].description(
                        auditLog,
                      ),
                    )
                  "
                />
              </div>
            </div>
          </lf-table-cell>

          <lf-table-cell class="pl-3">
            <div class="text-sm font-semibold text-black mb-1 leading-5">
              {{ auditLog.actor.fullName }}
            </div>
            <p v-if="auditLog.actor.email" class="text-2xs text-gray-500 leading-5">
              {{ auditLog.actor.email }}
            </p>
            <p class="text-2xs text-gray-500 leading-5">
              <span v-if="auditLog.actor.type === 'service'" class="capitalize">{{ auditLog.actor.type }} · </span>
              ID: {{ auditLog.actor.id }}
            </p>
          </lf-table-cell>

          <lf-table-cell class="pl-3">
            <div class="text-sm flex items-center h-full">
              {{ dateHelper(auditLog.timestamp).format("DD-MM-YYYY HH:mm:ss") }}
            </div>
          </lf-table-cell>

          <lf-table-cell class="pr-2">
            <div class="flex items-center justify-end h-full">
              <lf-button
                type="secondary"
                size="small"
                @click="() => openLogDetailsDrawer(auditLog)"
              >
                View details
              </lf-button>
            </div>
          </lf-table-cell>
        </tr>
      </tbody>
    </lf-table>
  </div>

  <div
    v-if="pagination.total >= pagination.page * pagination.perPage"
    class="pt-6 pb-6 flex justify-center"
  >
    <lf-button type="secondary-ghost" @click="loadMore">
      <lf-icon name="arrow-down" /> Load more
    </lf-button>
  </div>

  <app-lf-audit-logs-drawer v-model:log="openLogDetails" />
</template>

<script setup lang="ts">
import LfFilter from '@/shared/modules/filters/components/Filter.vue';
import { auditLogsFilters } from '@/modules/lf/config/audit-logs/filters/main';
import { FilterQuery } from '@/shared/modules/filters/types/FilterQuery';
import { reactive, ref } from 'vue';
import { Filter } from '@/shared/modules/filters/types/FilterConfig';
import AppLfAuditLogsDrawer from '@/modules/lf/segments/components/logs/log.drawer.vue';
import { AuditLog } from '@/modules/lf/segments/types/AuditLog';
import LfButton from '@/ui-kit/button/Button.vue';
import { LfService } from '@/modules/lf/segments/lf-segments-service';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';
import LfTable from '@/ui-kit/table/Table.vue';
import LfTableCell from '@/ui-kit/table/TableCell.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import { logRenderingConfig } from '../../config/audit-logs/log-rendering';

const loading = ref<boolean>(false);

const pagination = reactive({
  page: 1,
  perPage: 20,
  total: 0,
});

const filters = ref<Filter>({
  order: {
    order: 'descending',
    prop: 'createdAt',
  },
  relation: 'and',
  search: '',
});

const openLogDetails = ref<AuditLog | null>(null);

const auditLogs = ref<AuditLog[]>([]);
const filter = ref({});

const { trackEvent } = useProductTracking();

const loadMore = () => {
  pagination.page += 1;
  fetch();
};

const onFilterChange = (filterQuery: FilterQuery) => {
  trackEvent({
    key: FeatureEventKey.FILTER_AUDIT_LOGS,
    type: EventType.FEATURE,
    properties: {
      filter: filterQuery.filter,
    },
  });

  filter.value = filterQuery.filter;
  pagination.page = 1;
  pagination.total = 0;
  fetch();
};

const fetch = () => {
  const query = filter.value.and?.reduce(
    (q: any, filtr: any) => ({
      ...q,
      ...filtr,
    }),
    {},
  ) || {};
  if (pagination.page <= 1) {
    loading.value = true;
  }
  LfService.fetchAuditLogs({
    filter: query,
    limit: pagination.perPage,
    offset: (pagination.page - 1) * pagination.perPage,
  })
    .then((res: AuditLog[]) => {
      if (pagination.page > 1) {
        auditLogs.value = [...auditLogs.value, ...res];
      } else {
        auditLogs.value = res;
      }
      pagination.total = auditLogs.value.length;
    })
    .catch(() => {
      auditLogs.value = [];
    })
    .finally(() => {
      loading.value = false;
    });
};

const openLogDetailsDrawer = (log: AuditLog) => {
  openLogDetails.value = log;

  trackEvent({
    key: FeatureEventKey.VIEW_AUDIT_LOG_DETAILS,
    type: EventType.FEATURE,
  });
};
</script>

<script lang="ts">
export default {
  name: 'AppLfAuditLogsPage',
};
</script>
