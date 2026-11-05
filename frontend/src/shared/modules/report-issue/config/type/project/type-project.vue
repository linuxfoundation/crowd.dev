<template>
  <p class="text-medium">
    {{ props.attribute.name }}
  </p>
</template>

<script setup lang="ts">
import type { ReportDataProject } from '@/shared/modules/report-issue/config';

const props = defineProps<{
  attribute: ReportDataProject
}>();
</script>
