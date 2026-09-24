<template>
  <lf-modal v-model="isModalOpen" width="37.5rem">
    <div class="px-6 pt-5 pb-6">
      <!-- Header -->
      <div class="flex justify-between pb-8">
        <div class="pt-1 flex items-center gap-2">
          <div class="border border-gray-200 rounded-md h-12 w-12 p-2">
            <img
              :src="githubImage"
              alt="GitHub"
              class="min-w-8 h-8 object-contain"
            />
          </div>
          <div>
            <lf-icon name="angle-right" :size="20" class="text-gray-500" />
          </div>
          <div class="border border-gray-200 rounded-md h-12 w-12 p-2">
            <img :src="lfxImage" alt="LFX" class="min-w-8 h-8 object-contain" />
          </div>
        </div>
        <lf-button
          type="secondary-ghost"
          icon-only
          @click="isModalOpen = false"
        >
          <lf-icon name="xmark" type="regular" />
        </lf-button>
      </div>

      <!-- Content -->
      <h5 class="pb-3">
        GitHub integration
      </h5>
      <p class="text-small pb-6">
        GitHub is a code hosting platform that over 80 million developers use
        for collaboration and version control. Its one of the richest places for
        developer activity and information.
      </p>
      <p class="text-small font-semibold pb-1.5">
        Activities tracked
      </p>
      <p class="text-gray-600 text-small pb-3">
        Star counts, Forks, Issues, Pull requests, Discussions, Comments on
        issues, Pull requests, Discussions, Closing of issues/pull
        requests/discussions
      </p>
      <div class="border-t border-gray-200 pb-3" />
      <p class="text-small font-semibold pb-1.5">
        Historical import
      </p>
      <p class="text-gray-600 text-small pb-3">
        No limitation
      </p>
      <div class="border-t border-gray-200 pb-3" />
      <p class="text-small font-semibold pb-1.5">
        Refresh period
      </p>
      <p class="text-gray-600 text-small pb-10">
        Instant (maximum a few seconds)
      </p>
      <h6 class="pb-6">
        How to connect
      </h6>
      <div class="flex pb-6">
        <div
          class="w-6 h-6 rounded-full bg-gray-200 font-secondary text-center text-medium flex items-center justify-center"
        >
          1
        </div>
        <div class="pl-2">
          <p class="text-small leading-6 font-semibold pb-0.5">
            Select the repositories you want to track
          </p>
          <p class="text-tiny text-gray-500">
            Esse et nihil ratione sit facere aut fugiat corporis et animi velit
            natus laudantium et.
          </p>
        </div>
      </div>
      <div class="flex pb-10">
        <div
          class="w-6 h-6 rounded-full bg-gray-200 font-secondary text-center text-medium flex items-center justify-center"
        >
          2
        </div>
        <div class="pl-2">
          <p class="text-small leading-6 font-semibold pb-0.5">
            Map each repository with the correspondent project
          </p>
          <p class="text-tiny text-gray-500">
            In vero sed libero labore voluptatem eos ut libero consequuntur
            dolorem aut ut similique.
          </p>
        </div>
      </div>
      <lf-button
        type="secondary"
        size="large"
        class="w-full"
        @click="emit('connect')"
      >
        <lf-icon name="link-simple" type="regular" />
        Connect GitHub
      </lf-button>
    </div>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import { computed } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  modelValue: boolean;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'connect'): void;
}>();

const isModalOpen = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
});

const githubImage = new URL(
  '@/assets/images/integrations/github.png',
  import.meta.url,
).href;

const lfxImage = new URL(
  '@/assets/images/integrations/custom/lfx.png',
  import.meta.url,
).href;
</script>

<script lang="ts">
export default {
  name: 'LfGithubDetailsModal',
};
</script>
