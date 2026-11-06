<template>
  <app-page-wrapper>
    <div class="settings">
      <h4>
        Manage workspace
      </h4>
      <el-tabs v-model="computedActiveTab" class="mt-10">
        <el-tab-pane label="API Keys" name="api-keys">
          <app-api-keys-page
            v-if="activeTab === 'api-keys'"
          />
        </el-tab-pane>
      </el-tabs>
    </div>
  </app-page-wrapper>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import AppApiKeysPage from '@/modules/settings/pages/api-keys-page.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';

defineOptions({ name: 'AppSettingsPage' });

const route = useRoute();
const router = useRouter();
const { updateSelectedProjectGroup } = useLfSegmentsStore();

updateSelectedProjectGroup(null);
const params = Object.fromEntries(
  new URLSearchParams(window.location.search).entries(),
);
const activeTab = ref<string>(params.activeTab || 'api-keys');

const computedActiveTab = computed({
  get: () => activeTab.value,
  set: (value: string) => {
    router.push({
      name: 'settings',
      query: { activeTab: value },
    });
  },
});

watch(() => route.query.activeTab, (newActiveTab) => {
  if (newActiveTab) {
    activeTab.value = newActiveTab as string;
  }
});
</script>

<style lang="scss">
.el-tabs {
  &__item {
    @apply font-normal text-black;
    &.is-active {
      @apply text-black;
    }
  }
}
</style>
