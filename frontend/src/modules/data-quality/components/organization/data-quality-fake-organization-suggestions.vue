<template>
  <div>
    <div v-if="loading && offset === 0" class="flex justify-center py-20">
      <lf-spinner />
    </div>
    <lf-scroll-body-controll v-else-if="suggestions.length > 0" @bottom="loadMore()">
      <lf-data-quality-fake-organization-suggestion-item
        v-for="(suggestion, si) of suggestions"
        :key="suggestion.organizationId"
        :suggestion="suggestion"
      >
        <template #action>
          <div class="flex gap-3">
            <lf-button
              type="secondary"
              size="small"
              @click="isModalOpen = true; detailsOffset = si"
            >
              <lf-icon name="eye" />View suggestion
            </lf-button>
            <lf-fake-organization-suggestion-dropdown
              :suggestion="suggestion"
              @dismiss="dismiss(suggestion)"
            />
          </div>
        </template>
      </lf-data-quality-fake-organization-suggestion-item>
      <div v-if="suggestions.length < total" class="pt-4">
        <lf-button
          type="primary-ghost"
          loading-text="Loading suggestions..."
          :loading="loading"
          @click="loadMore()"
        >
          Load more
        </lf-button>
      </div>
    </lf-scroll-body-controll>
    <div v-else class="flex flex-col items-center pt-16">
      <lf-icon name="shuffle" :size="160" class="text-gray-200 flex items-center mb-8" />
      <h5 class="text-center text-lg font-semibold mb-4">
        No fake organization suggestions
      </h5>
      <p class="text-sm text-center text-gray-600 leading-5">
        We couldn't find any fake organization suggestions
      </p>
    </div>
  </div>
  <app-fake-organization-suggestions-dialog
    v-model="isModalOpen"
    :offset="detailsOffset"
    :segments="segments"
    @reload="reload()"
  />
</template>

<script lang="ts" setup>
import { OrganizationService } from '@/modules/organization/organization-service';
import {
  computed, onMounted, ref, watch,
} from 'vue';
import LfDataQualityFakeOrganizationSuggestionItem
  from '@/modules/data-quality/components/organization/data-quality-fake-organization-suggestion-item.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { ToastStore } from '@/shared/message/notification';
import LfFakeOrganizationSuggestionDropdown
  from '@/modules/organization/components/suggestions/fake-organization-suggestion-dropdown.vue';
import AppFakeOrganizationSuggestionsDialog
  from '@/modules/organization/components/fake-organization-suggestions-dialog.vue';
import LfScrollBodyControll from '@/ui-kit/scrollcontroll/ScrollBodyControll.vue';

const props = defineProps<{
  projectGroup: string,
}>();

const loading = ref(true);
const limit = ref(20);
const offset = ref(0);
const total = ref(0);
const suggestions = ref<any[]>([]);
const itemsLoading = ref<Record<string, boolean>>({});
const isModalOpen = ref(false);
const detailsOffset = ref(0);

const segments = computed(() => [props.projectGroup]);

const loadSuggestions = () => {
  loading.value = true;

  OrganizationService.fetchFakeOrganizationSuggestions(limit.value, offset.value, {
    segments: segments.value,
    detail: false,
  })
    .then((res) => {
      total.value = +res.count;
      if (+res.offset > 0) {
        suggestions.value = [...suggestions.value, ...res.rows];
      } else {
        suggestions.value = res.rows;
      }
    })
    .finally(() => {
      loading.value = false;
    });
};

const reload = () => {
  offset.value = 0;
  loadSuggestions();
};

const dismiss = (suggestion: any) => {
  itemsLoading.value[suggestion.organizationId] = true;

  OrganizationService.dismissFakeOrganizationSuggestion(suggestion.organizationId)
    .then(() => {
      reload();
    })
    .catch((err) => {
      ToastStore.error(err.response.data);
    })
    .finally(() => {
      itemsLoading.value[suggestion.organizationId] = false;
    });
};

const loadMore = () => {
  if (loading.value || suggestions.value.length >= total.value) {
    return;
  }

  offset.value = suggestions.value.length;
  loadSuggestions();
};

watch(() => props.projectGroup, () => {
  offset.value = 0;
  loadSuggestions();
});

onMounted(() => {
  loadSuggestions();
});
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityFakeOrganizationSuggestions',
};
</script>
