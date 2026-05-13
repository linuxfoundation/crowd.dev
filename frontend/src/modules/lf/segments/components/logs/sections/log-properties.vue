<template>
  <section class="py-6">
    <!-- Status -->
    <article class="flex pb-4">
      <div class="w-5/12 text-sm leading-6 font-medium text-gray-500">
        Status
      </div>
      <div class="w-7/12 text-sm leading-24">
        <div v-if="props.log.success" class="flex items-center">
          <lf-icon name="circle-check" type="solid" :size="16" class="text-green-500 mr-1" />
          Success
        </div>
        <div v-else class="flex items-center">
          <lf-icon name="circle-xmark" type="solid" :size="16" class="text-red-500 mr-1" />
          Error
        </div>
      </div>
    </article>

    <!-- Action -->
    <article v-if="props.log.actionType" class="flex pb-4">
      <div class="w-5/12 text-sm leading-6 font-medium text-gray-500">
        Action
      </div>
      <div class="w-7/12 text-sm leading-24">
        {{ config?.label }}
      </div>
    </article>

    <!-- Entity type -->
    <article v-if="props.log.actionType" class="flex pb-4">
      <div class="w-5/12 text-sm leading-6 font-medium text-gray-500">
        Entity type
      </div>
      <div class="w-7/12 text-sm leading-24">
        <span class="capitalize">{{ getEntityType(props.log.actionType) }}</span>
      </div>
    </article>

    <!-- Custom properties -->
    <template v-if="config.properties">
      <article v-for="property of config.properties(props.log)" :key="property.label" class="flex pb-4">
        <div class="w-5/12 text-sm leading-6 font-medium text-gray-500">
          {{ property.label }}
        </div>
        <div class="w-7/12 text-sm leading-24 c-property" v-html="property.value" />
      </article>
    </template>

    <!-- Timestamp -->
    <article v-if="props.log.timestamp" class="flex pb-4">
      <div class="w-5/12 text-sm leading-6 font-medium text-gray-500">
        Timestamp
      </div>
      <div class="w-7/12 text-sm leading-24">
        {{ dateHelper(props.log.timestamp).format('DD-MM-YYYY HH:mm:ss') }}
      </div>
    </article>

    <!-- Actor -->
    <article class="flex pb-4">
      <div class="w-5/12 text-sm leading-6 font-medium text-gray-500">
        Actor
      </div>
      <div class="w-7/12 text-sm leading-24">
        {{ props.log.actor.fullName }}
        <p v-if="props.log.actor.email" class="text-2xs text-gray-500">
          {{ props.log.actor.email }}
        </p>
        <p class="text-2xs text-gray-500">
          <span v-if="props.log.actor.type === 'service'" class="capitalize">{{ props.log.actor.type }} · </span>
          ID: {{ props.log.actor.id }}
        </p>
      </div>
    </article>

    <!-- IP Address -->
    <article v-if="props.log.ipAddress" class="flex pb-4">
      <div class="w-5/12 text-sm leading-6 font-medium text-gray-500">
        IP Address
      </div>
      <div class="w-7/12 text-sm leading-24">
        {{ props.log.ipAddress }}
      </div>
    </article>

    <!-- Entity type -->
    <article v-if="props.log.userAgent" class="flex pb-4">
      <div class="w-5/12 text-sm leading-6 font-medium text-gray-500">
        Device
      </div>
      <div class="w-7/12 text-sm leading-24">
        {{ props.log.userAgent }}
      </div>
    </article>
  </section>
</template>

<script setup lang="ts">
import { ActionType, AuditLog } from '@/modules/lf/segments/types/AuditLog';
import { computed } from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';
import { logRenderingConfig } from '../../../../config/audit-logs/log-rendering';

const props = defineProps<{
  log: AuditLog
}>();

const getEntityType = (actionType: ActionType) => {
  const [entityType] = actionType.split('-');
  return entityType.slice(0, -1);
};

const config = computed(() => logRenderingConfig[props.log.actionType]);
</script>

<script lang="ts">
export default {
  name: 'AppLfAuditLogsProperties',
};
</script>

<style lang="scss">
.c-property{
  span{
    @apply text-2xs text-gray-500;
  }
}
</style>
