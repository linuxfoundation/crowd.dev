<template>
  <div>
    <div v-if="loading && offset === 0" class="flex justify-center py-20">
      <lf-spinner />
    </div>
    <lf-scroll-body-controll v-else-if="botSuggestions.length > 0" @bottom="loadMore()">
      <lf-data-quality-member-bot-suggestion-item
        v-for="(suggestion, si) of botSuggestions"
        :key="suggestion.memberId"
        :suggestion="suggestion"
      >
        <template #action>
          <div class="flex gap-3">
            <lf-button type="secondary" size="small" @click="isModalOpen = true; detailsOffset = si">
              <lf-icon name="eye" />View suggestion
            </lf-button>
            <lf-member-bot-suggestion-dropdown :suggestion="suggestion" @reload="reload()" @ignore-suggestion="ignoreSuggestion(suggestion)" />
          </div>
        </template>
      </lf-data-quality-member-bot-suggestion-item>
      <div v-if="botSuggestions.length < total" class="pt-4">
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
        No bot suggestions
      </h5>
      <p class="text-sm text-center text-gray-600 leading-5">
        We couldn't find any bot suggestions
      </p>
    </div>
  </div>
  <app-member-bot-suggestions-dialog
    v-model="isModalOpen"
    :offset="detailsOffset"
    :segments="segments"
    @reload="reload()"
  />
</template>

<script lang="ts" setup>
import { MemberService } from '@/modules/member/member-service';
import {
  computed, onMounted, ref, watch,
} from 'vue';
import LfDataQualityMemberBotSuggestionItem
  from '@/modules/data-quality/components/member/data-quality-member-bot-suggestion-item.vue';
import LfSpinner from '@/ui-kit/spinner/Spinner.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { ToastStore } from '@/shared/message/notification';
import LfMemberBotSuggestionDropdown
  from '@/modules/member/components/suggestions/member-bot-suggestion-dropdown.vue';
import AppMemberBotSuggestionsDialog
  from '@/modules/member/components/member-bot-suggestions-dialog.vue';
import LfScrollBodyControll from '@/ui-kit/scrollcontroll/ScrollBodyControll.vue';

const props = defineProps<{
  projectGroup: string,
}>();

const loading = ref(true);
const limit = ref(20);
const offset = ref(0);
const total = ref(0);
const botSuggestions = ref<any[]>([]);
const itemsLoading = ref<any>({});
const isModalOpen = ref(false);
const detailsOffset = ref(0);

const segments = computed(() => [props.projectGroup]);

const loadBotSuggestions = () => {
  loading.value = true;

  MemberService.fetchBotSuggestions(limit.value, offset.value, {
    segments: segments.value,
    detail: false,
  })
    .then((res) => {
      total.value = +res.count;
      if (+res.offset > 0) {
        botSuggestions.value = [...botSuggestions.value, ...res.rows];
      } else {
        botSuggestions.value = res.rows;
      }
    })
    .finally(() => {
      loading.value = false;
    });
};

const ignoreSuggestion = (suggestion: any) => {
  itemsLoading.value[suggestion.memberId] = true;

  MemberService.updateAttributes(suggestion.memberId, {
    ...suggestion.attributes,
    isBot: {
      custom: false,
      default: false,
    },
  }).then(() => {
    reload();
  }).catch((err) => {
    ToastStore.error(err.response.data);
  }).finally(() => {
    itemsLoading.value[suggestion.memberId] = false;
  });
};

const loadMore = () => {
  if (loading.value || botSuggestions.value.length >= total.value) {
    return;
  }

  offset.value = botSuggestions.value.length;
  loadBotSuggestions();
};

const reload = () => {
  offset.value = 0;
  loadBotSuggestions();
};

watch(() => props.projectGroup, () => {
  offset.value = 0;
  loadBotSuggestions();
});

onMounted(() => {
  loadBotSuggestions();
});
</script>

<script lang="ts">
export default {
  name: 'LfDataQualityMemberBotSuggestions',
};
</script>
