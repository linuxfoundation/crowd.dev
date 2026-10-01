<template>
  <app-drawer
    v-model="isDrawerOpen"
    size="480px"
    title="Log details"
    custom-class="identities-drawer"
    :show-footer="false"
  >
    <template #content>
      <div class="border-t border-gray-100 -mx-6 px-6 -mt-4">
        <app-lf-audit-logs-properties v-if="props.log" :log="props.log" />
        <app-lf-audit-logs-changes v-if="props.log && props.log.success" :log="props.log" />
        <app-lf-audit-logs-json v-if="props.log" :log="props.log" />
      </div>
    </template>
  </app-drawer>
</template>

<script setup lang="ts">
import type { AuditLog } from '@/modules/lf/segments/types/AuditLog';
import { computed } from 'vue';
import AppDrawer from '@/shared/drawer/drawer.vue';
import AppLfAuditLogsJson from '@/modules/lf/segments/components/logs/sections/log-json.vue';
import AppLfAuditLogsProperties from '@/modules/lf/segments/components/logs/sections/log-properties.vue';
import AppLfAuditLogsChanges from '@/modules/lf/segments/components/logs/sections/log-changes.vue';

const props = defineProps<{
  log: AuditLog | null
}>();

const emit = defineEmits<{(e: 'update:log', value: AuditLog | null): void}>();

const isDrawerOpen = computed<boolean>({
  get() {
    return !!props.log;
  },
  set(open: boolean) {
    emit('update:log', open ? props.log : null);
  },
});
</script>

<script lang="ts">
export default {
  name: 'AppLfAuditLogsDrawer',
};
</script>
