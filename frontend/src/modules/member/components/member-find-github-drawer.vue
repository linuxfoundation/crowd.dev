<template>
  <app-drawer
    v-model="drawerModel"
    size="35%"
    :title="title"
    custom-class="identities-drawer"
  >
    <template #content>
      <div v-if="suggestions.length > 0">
        <div
          v-for="suggestion in suggestions"
          :key="suggestion.url"
          class="flex items-center"
        >
          <div
            class="py-2 flex justify-between w-full px-4 cursor-pointer hover:bg-gray-50 rounded-md hover:shadow-sm"
            :class="{ selected: selected === suggestion.username }"
            @click="changeSelected(suggestion.username)"
          >
            <div class="flex">
              <app-avatar
                :entity="{
                  displayName: suggestion.username,
                  avatar: suggestion.avatarUrl,
                }"
                size="xs"
                class="mr-3"
              />
              <div class="flex flex-col justify-center">
                <p class="text-xs leading-4.5" v-html="$sanitize(suggestion.username)" />
              </div>
            </div>
            <div class="pt-1">
              <a :href="suggestion.url" target="_blank" rel="noopener noreferrer" class="text-gray-300">
                <lf-icon name="arrow-up-right-from-square" :size="16" class="text-gray-400" />
                <span class="sr-only">Open in new tab</span>
              </a>
            </div>
          </div>
        </div>
        <div class="mt-4">
          <lf-button
            size="medium"
            type="bordered"
            @click="searchContactOnGoogle"
          >
            Not a match? Search Google instead
          </lf-button>
        </div>
      </div>
      <div v-else-if="!loading">
        <app-empty-state-cta
          icon="user-group-simple"
          title="We could not find any GitHub identities for this person."
          description="There are no GitHub users that match the identities in this person. We recommend Googling them instead."
          secondary-btn="Search on Google"
          @secondary-click="searchContactOnGoogle"
        />
      </div>
    </template>
    <template #footer>
      <div style="flex: auto">
        <lf-button
          type="bordered"
          size="medium"
          class="mr-3"
          @click="handleCancel"
        >
          Cancel
        </lf-button>
        <lf-button
          type="primary"
          size="medium"
          :disabled="isSubmitBtnDisabled || loading"
          :loading="loading"
          @click="handleSubmit"
        >
          Update
        </lf-button>
      </div>
    </template>
  </app-drawer>
</template>

<script setup>
import { useStore } from 'vuex';
import {
  ref,
  computed,
  onMounted,
  reactive,
} from 'vue';

import { ToastStore } from '@/shared/message/notification';
import { MemberService } from '@/modules/member/member-service';
import cloneDeep from 'lodash/cloneDeep';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import AppEmptyStateCta from '@/shared/empty-state/empty-state-cta.vue';
import LfButton from '@/ui-kit/button/Button.vue';

const props = defineProps({
  modelValue: {
    type: Object,
    required: true,
  },
});
const emit = defineEmits(['update:modelValue']);
const store = useStore();
const memberModel = reactive(cloneDeep(props.modelValue));

const suggestions = ref([]);
const selected = ref('');

onMounted(async () => {
  loading.value = true;
  suggestions.value = await MemberService.findGithub(props.modelValue.id);
  loading.value = false;
});

const drawerModel = computed({
  get() {
    return !!props.modelValue;
  },
  set() {
    emit('update:modelValue', null);
  },
});

const title = computed(() => `Find the GitHub identity for ${props.modelValue.displayName}`);

const loading = ref(false);

const handleCancel = () => {
  emit('update:modelValue', false);
};

const changeSelected = (username) => {
  selected.value = username;
};

const searchContactOnGoogle = () => {
  const searchTerms = new Set([props.modelValue.displayName.toLowerCase()]);
  Object.keys(props.modelValue.username).forEach((username) => {
    if (Object.prototype.hasOwnProperty.call(props.modelValue.username, username)) {
      props.modelValue.username[username].forEach((user) => {
        const lowerCaseUser = user.toLowerCase();
        if (!searchTerms.has(lowerCaseUser)) {
          searchTerms.add(lowerCaseUser);
        }
      });
    }
  });
  const searchQuery = `(${Array.from(searchTerms).join(' OR ')}) github`;
  window.open(`https://www.google.com/search?q=${searchQuery}`, '_blank');
};

const handleSubmit = async () => {
  loading.value = true;

  MemberService.update(props.modelValue.id, {
    username: { ...memberModel.username, github: [selected.value] },
  }).then(() => {
    store.dispatch('member/doFind', props.modelValue.id).then(() => {
      ToastStore.success('GitHub added successfully');
    });
  }).catch((err) => {
    ToastStore.error(err.response.data);
  }).finally(() => {
    loading.value = false;
  });

  emit('update:modelValue', false);
};
</script>

<script>
export default {
  name: 'AppMemberFindGithbDrawer',
};
</script>

<style lang="scss">
.identities-drawer {
  .el-form-item,
  .el-form-item__content {
    @apply mb-0;
  }
}

.selected {
  @apply bg-primary-50 border-primary-400 hover:bg-primary-50 hover:border-primary-400
}

</style>
