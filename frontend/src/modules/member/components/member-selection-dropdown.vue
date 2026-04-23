<template>
  <div class="pt-16">
    <div class="flex justify-center">
      <lf-icon name="circle-user" :size="64" class="text-gray-200 account-icon text-center flex items-center" />
    </div>
    <div class="text-gray-600 text-sm text-center py-4">
      Select the profile you want to merge with
    </div>
    <div class="flex justify-center">
      <div class="flex w-4/5">
        <app-autocomplete-one-input
          id="searchMembers"
          v-model="computedMemberToMerge"
          :fetch-fn="fetchFn"
          placeholder="Search people"
          input-class="w-full"
        >
          <template #option="{ item }">
            <div class="flex items-center">
              <app-avatar
                :entity="{
                  displayName: item.label,
                  avatar: item.logo,
                }"
                size="xs"
                class="mr-3"
              />
              <div class="flex flex-col justify-center">
                <p class="text-xs leading-4.5" v-html="$sanitize(item.label)" />
                <p v-if="item.organizations.length > 0" class="text-xxs leading-4.5 text-gray-500">
                  {{ item.organizations.map((o) => o.name).join(', ') }}
                </p>
              </div>
            </div>
          </template>
        </app-autocomplete-one-input>
      </div>
    </div>
  </div>
</template>

<script setup>
import {
  computed,
  ref,
  defineProps,
  defineEmits,
} from 'vue';
import { MemberService } from '@/modules/member/member-service';
import AppAutocompleteOneInput from '@/shared/form/autocomplete-one-input.vue';
import AppAvatar from '@/shared/avatar/avatar.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const emit = defineEmits('update:modelValue');
const props = defineProps({
  modelValue: {
    type: Object,
    required: true,
  },
  id: {
    type: String,
    required: true,
  },
});
const loadingMemberToMerge = ref();
const computedMemberToMerge = computed({
  get() {
    return props.modelValue;
  },
  async set(value) {
    loadingMemberToMerge.value = true;

    const response = await MemberService.find(value.id);

    emit('update:modelValue', response);
    loadingMemberToMerge.value = false;
  },
});

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const fetchFn = async ({ query, limit }) => {
  const options = await MemberService.listMembersAutocomplete({
    query,
    limit,
    segments: [selectedProjectGroup.value.id],
  });

  // Remove primary member from members that can be merged with
  const filteredOptions = options.filter((m) => m.id !== props.id);

  // If the primary member was removed, add an empty object in replacement
  if (options.length !== filteredOptions.length) {
    filteredOptions.push({});
  }

  return filteredOptions;
};
</script>

<style lang="scss" scoped>
.account-icon {
  font-size: 64px;
}
#searchMembers .el-select-dropdown__item {
  height: auto !important;
}
</style>
