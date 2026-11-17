<template>
  <app-page-wrapper size="narrow">
    <div class="activity-list-page">
      <app-lf-page-header text-class="text-sm text-primary-600 mb-2.5" />
      <div class="flex justify-between">
        <div>
          <h4>Activities</h4>
          <div class="text-xs text-gray-500 mb-10">
            Activities are all interactions with your company brand, community, and product
          </div>
        </div>
      </div>

      <div class="relative">
        <el-tabs :model-value="activeView" class="mb-6" @update:model-value="changeView">
          <el-tab-pane
            label="Activities"
            name="activity"
          />
        </el-tabs>
      </div>
      <app-activity-list
        v-if="activeView === 'activity'"
      />
    </div>
  </app-page-wrapper>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import AppActivityList from '@/modules/activity/components/activity-list.vue';
import AppLfPageHeader from '@/modules/lf/layout/components/lf-page-header.vue';
import { analytics } from '@/utils/segment';

const route = useRoute();
const router = useRouter();

const activeView = ref('activity');

onMounted(() => {
  analytics.page('Activities');
});

const changeView = (view: string) => {
  router.push({
    hash: `#${view}`,
    query: {},
  });
};

watch(() => route.hash, (hash: string) => {
  const view = hash.substring(1);
  if (view.length > 0 && view !== activeView.value) {
    activeView.value = view;
  }
}, { immediate: true });
</script>
