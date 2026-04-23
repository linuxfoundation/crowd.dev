<template>
  <div>
    <div v-if="loading && offset === 0" class="flex justify-center py-20">
      <lf-spinner />
    </div>
    <lf-scroll-body-controll v-else-if="mergeSuggestions.length > 0" @bottom="loadMore()">
      <lf-data-quality-member-merge-suggestions-item
        v-for="(suggestion, si) of mergeSuggestions"
        :key="suggestion.id"
        :suggestion="suggestion"
      >
        <template #action>
          <div class="flex gap-3">
            <lf-button type="secondary" size="small" @click="isModalOpen = true; detailsOffset = si">
              <lf-icon name="eye" />View suggestion
            </lf-button>
            <lf-member-merge-suggestion-dropdown :suggestion="suggestion" @reload="reload()" />
          </div>
        </template>
      </lf-data-quality-member-merge-suggestions-item>
      <div v-if="mergeSuggestions.length < total" class="pt-4">
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
        No merge suggestions
      </h5>
      <p class="text-sm text-center text-gray-600 leading-5">
        We couldn't find any duplicated profiles
      </p>
    </div>
  </div>
  <app-member-merge-suggestions-dialog
    v-model="isModalOpen"
    :offset="detailsOffset"
    :query="{
      orderBy: ['similarity_DESC', 'activityCount_DESC'],
      filter: { similarity: ['high'] },
      segments: segments,
    }"
    @reload="offset = 0; loadMergeSuggestions()"
  />
</template>

<script lang="ts" setup>
import { MemberService } from '@/modules/member/member-service';
import {
  computed, onMounted, ref, watch,
} from 'vue';
import LfDataQualityMemberMergeSuggestionsItem
  from '@/modules/data-quality/components/member/data-quality-member-merge-suggestions-item.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import AppMemberMergeSuggestionsDialog from '@/modules/member/components/member-merge-suggestions-dialog.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfMemberMergeSuggestionDropdown
  from '@/modules/member/components/suggestions/member-merge-suggestion-dropdown.vue';
import LfScrollBodyControll from '@/ui-kit/scrollcontroll/ScrollBodyControll.vue';

const props = defineProps<{
  projectGroup: string,
}>();

const loading = ref(true);
const limit = ref(20);
const offset = ref(0);
const total = ref(0);
const mergeSuggestions = ref<any[]>([]);

const isModalOpen = ref<boolean>(false);
const detailsOffset = ref<number>(0);

const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());

const segments = computed(() => (selectedProjectGroup.value?.id === props.projectGroup
  ? [
    selectedProjectGroup.value?.id,
    ...selectedProjectGroup.value.projects.map((p) => [
      ...p.subprojects.map((sp) => sp.id),
    ]).flat(),
  ]
  : [
    props.projectGroup,
    ...selectedProjectGroup.value.projects
      .filter((p) => p.id === props.projectGroup)
      .map((p) => [
        ...p.subprojects.map((sp) => sp.id),
      ]).flat(),
  ]));

const loadMergeSuggestions = () => {
  loading.value = true;

  MemberService.fetchMergeSuggestions(limit.value, offset.value, {
    detail: false,
    filter: { similarity: ['high'] },
    orderBy: ['similarity_DESC', 'activityCount_DESC'],
    segments: segments.value,
  })
    .then((res) => {
      total.value = +res.count;
      const rows = res.rows.filter((s: any) => s.similarity > 0);
      if (+res.offset > 0) {
        mergeSuggestions.value = [...mergeSuggestions.value, ...rows];
      } else {
        mergeSuggestions.value = rows;
      }
    })
    .finally(() => {
      loading.value = false;
    });
};

const loadMore = () => {
  offset.value = mergeSuggestions.value.length;
  loadMergeSuggestions();
};

const reload = () => {
  offset.value = 0;
  loadMergeSuggestions();
};

watch(() => props.projectGroup, () => {
  offset.value = 0;
  loadMergeSuggestions();
});

onMounted(() => {
  loadMergeSuggestions();
});
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityMemberMergeSuggestions',
};
</script>
