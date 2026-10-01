<template>
  <!-- Project name -->
  <article class="mb-5">
    <lf-field label-text="Project name" :required="true">
      <template #label>
        <label class="c-field__label leading-5">
          Project name <span class="c-field__required">*</span>
        </label>
        <div
          v-if="newForm && oldForm?.name !== newForm.name"
          class="flex items-center p-1 rounded-md bg-gray-100 gap-2 cursor-pointer text-gray-500 text-tiny font-semibold"
        >
          <span
            class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
            :class="useOldData.name ? 'bg-white text-gray-900' : ''"
            @click="!useOldData.name ? onUseOldData(['name']) : null"
          >Old data</span>
          <span
            class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
            :class="useOldData.name ? '' : 'bg-white text-gray-900'"
            @click="useOldData.name ? useNewData(['name']) : null"
          >updated data</span>
        </div>
      </template>
      <lf-input
        v-model="cForm.name"
        class="h-10"
        :invalid="$v.name.$invalid && $v.name.$dirty"
        @blur="$v.name.$touch()"
        @change="$v.name.$touch()"
      />
      <lf-field-messages
        :validation="$v.name"
        :error-messages="{ required: 'This field is required' }"
      />
    </lf-field>
  </article>

  <!-- Description -->
  <article class="mb-5">
    <lf-field label-text="Description" :required="true">
      <template #label>
        <label class="c-field__label leading-5">
          Description <span class="c-field__required">*</span>
        </label>
        <div
          v-if="newForm && oldForm?.description !== newForm.description"
          class="flex items-center p-1 rounded-md bg-gray-100 gap-2 cursor-pointer text-gray-500 text-tiny font-semibold"
        >
          <span
            class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
            :class="useOldData.description ? 'bg-white text-gray-900' : ''"
            @click="!useOldData.description ? onUseOldData(['description']) : null"
          >Old data</span>
          <span
            class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
            :class="useOldData.description ? '' : 'bg-white text-gray-900'"
            @click="useOldData.description ? useNewData(['description']) : null"
          >updated data</span>
        </div>
      </template>
      <lf-textarea
        v-model="cForm.description"
        :invalid="$v.description.$invalid && $v.description.$dirty"
        @blur="$v.description.$touch()"
        @change="$v.description.$touch()"
      />
      <lf-field-messages
        :validation="$v.description"
        :error-messages="{ required: 'This field is required' }"
      />
    </lf-field>
  </article>

  <!-- Logo -->
  <article class="mb-5">
    <lf-field label-text="Logo URL" :required="true">
      <template #label>
        <label class="c-field__label leading-5">
          Logo URL <span class="c-field__required">*</span>
        </label>
        <div
          v-if="newForm && oldForm?.logoUrl !== newForm.logoUrl"
          class="flex items-center p-1 rounded-md bg-gray-100 gap-2 cursor-pointer text-gray-500 text-tiny font-semibold"
        >
          <span
            class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
            :class="useOldData.logoUrl ? 'bg-white text-gray-900' : ''"
            @click="!useOldData.logoUrl ? onUseOldData(['logoUrl']) : null"
          >Old data</span>
          <span
            class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
            :class="useOldData.logoUrl ? '' : 'bg-white text-gray-900'"
            @click="useOldData.logoUrl ? useNewData(['logoUrl']) : null"
          >updated data</span>
        </div>
      </template>
      <lf-input
        v-model="cForm.logoUrl"
        class="h-10"
        :invalid="$v.logoUrl.$invalid && $v.logoUrl.$dirty"
        @blur="$v.logoUrl.$touch()"
        @change="$v.logoUrl.$touch()"
      />
      <lf-field-messages
        :validation="$v.logoUrl"
        :error-messages="{ required: 'This field is required' }"
      />
    </lf-field>
  </article>

  <!-- topics -->
  <article class="mb-5">
    <lf-field label-text="Topics">
      <template #label>
        <label class="c-field__label leading-5"> Topics </label>
        <div
          v-if="newForm && isKeywordsDifferent()"
          class="flex items-center p-1 rounded-md bg-gray-100 gap-2 cursor-pointer text-gray-500 text-tiny font-semibold"
        >
          <span
            class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
            :class="useOldData.keywords ? 'bg-white text-gray-900' : ''"
            @click="!useOldData.keywords ? onUseOldData(['keywords']) : null"
          >Old data</span>
          <span
            class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
            :class="useOldData.keywords ? '' : 'bg-white text-gray-900'"
            @click="useOldData.keywords ? useNewData(['keywords']) : null"
          >updated data</span>
        </div>
      </template>
      <app-keywords-input
        v-model="cForm.keywords"
        :show-hint="true"
        placeholder="Enter topic(s)"
        hint-text="Press ENTER or comma (,) to separate topics."
      />
    </lf-field>
  </article>

  <!-- Collections -->
  <article class="mb-5">
    <lf-field label-text="Collections">
      <lf-insights-projects-add-collection-dropdown :form="cForm" />
    </lf-field>
  </article>

  <!-- Associated company -->
  <article class="mb-5">
    <lf-field label-text="Associated company">
      <lf-insights-projects-add-organizations-dropdown
        v-if="cForm.segmentId"
        :form="cForm"
      />
    </lf-field>
  </article>

  <!-- Keywords -->
  <article class="mb-5">
    <lf-field label-text="Keyword(s)">
      <div class="text-tiny text-gray-500 mb-1">
        <p class="mb-1">
          Add keywords to your project. These keywords will be used to search
          for repositories and projects that match the keywords.
        </p>
        <ul class="list-disc pl-4">
          <li>Social mentions</li>
          <li>Press mentions</li>
          <li>Search queries (powered by Google Trends)</li>
        </ul>
      </div>
      <app-keywords-input
        v-model="cForm.searchKeywords"
        placeholder="Enter keyword(s)"
        :show-hint="true"
      />
    </lf-field>
  </article>

  <hr class="my-5" />

  <div class="flex items-center justify-between mb-6">
    <h6 class="font-semibold">
      Website & Social accounts
    </h6>
    <div
      v-if="newForm && isWebsiteSectionDifferent()"
      class="flex items-center p-1 rounded-md bg-gray-100 gap-2 cursor-pointer text-gray-500 text-tiny font-semibold"
    >
      <span
        class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
        :class="useOldData.website ? 'bg-white text-gray-900' : ''"
        @click="
          !useOldData.website
            ? onUseOldData(['website', 'github', 'twitter'])
            : null
        "
      >Old data</span>
      <span
        class="hover:bg-gray-200 px-1.5 py-0.5 rounded-xmd"
        :class="useOldData.website ? '' : 'bg-white text-gray-900'"
        @click="
          useOldData.website ? useNewData(['website', 'github', 'twitter']) : null
        "
      >updated data</span>
    </div>
  </div>

  <!-- Website -->
  <article class="mb-5">
    <lf-field label-text="Website">
      <lf-input
        v-model="cForm.website"
        class="h-10"
        placeholder="https://www.example.com"
      />
      <lf-field-messages
        :validation="$v.website"
        :error-messages="{ required: 'This field is required' }"
      />
    </lf-field>
    <div class="flex items-center mt-6">
      <lf-icon class="mr-2.5" name="github" :size="25" type="brands" />
      <lf-input
        v-model="cForm.github"
        class="h-10 flex-grow"
        placeholder="Enter GitHub repository URL"
      />
    </div>
    <div class="flex items-center mt-3">
      <lf-icon
        class="mr-2.5 text-[#2867B2]"
        name="linkedin"
        :size="25"
        type="brands"
      />
      <lf-input
        v-model="cForm.linkedin"
        class="h-10 flex-grow"
        placeholder="Enter LinkedIn URL"
      />
    </div>
    <div class="flex items-center mt-3">
      <lf-icon class="mr-2.5" name="x-twitter" :size="25" type="brands" />
      <lf-input
        v-model="cForm.twitter"
        class="h-10 flex-grow"
        placeholder="Enter X/Twitter URL"
      />
    </div>
  </article>
</template>

<script setup lang="ts">
import LfField from '@/ui-kit/field/Field.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import LfTextarea from '@/ui-kit/textarea/Textarea.vue';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import useVuelidate from '@vuelidate/core';
import { reactive, ref } from 'vue';
import AppKeywordsInput from '@/shared/form/keywords-input.vue';
import type { InsightsProjectAddFormModel } from '../models/insights-project-add-form.model';
import LfInsightsProjectsAddCollectionDropdown from './add-details-tab/lf-insights-projects-add-collection-dropdown.vue';
import LfInsightsProjectsAddOrganizationsDropdown from './add-details-tab/lf-insights-projects-add-organizations-dropdown.vue';

const props = defineProps<{
  form: InsightsProjectAddFormModel;
  oldForm?: InsightsProjectAddFormModel;
  newForm?: InsightsProjectAddFormModel;
  rules: any;
}>();
const cForm = reactive(props.form);
const $v = useVuelidate(props.rules, cForm);
const useOldData = ref<Record<string, boolean>>({
  name: true,
  description: true,
  logoUrl: true,
  keywords: true,
  github: true,
  twitter: true,
  website: true,
});

const useNewData = (fields: string[]) => {
  fields.forEach((field) => {
    useOldData.value[field] = false;
    if (props.newForm) {
      cForm[field as keyof InsightsProjectAddFormModel] = props.newForm[
        field as keyof InsightsProjectAddFormModel
      ] as any;
    }
  });
};

const onUseOldData = (fields: string[]) => {
  fields.forEach((field) => {
    useOldData.value[field] = true;
    if (props.oldForm) {
      cForm[field as keyof InsightsProjectAddFormModel] = props.oldForm[
        field as keyof InsightsProjectAddFormModel
      ] as any;
    }
  });
};

const isKeywordsDifferent = () => {
  if (!props.oldForm || !props.newForm) return false;
  const oldKeywords = props.oldForm.keywords || [];
  const newKeywords = props.newForm.keywords || [];
  return (
    oldKeywords.length !== newKeywords.length
    || oldKeywords.some((keyword) => !newKeywords.includes(keyword))
    || newKeywords.some((keyword) => !oldKeywords.includes(keyword))
  );
};

const isWebsiteSectionDifferent = () => {
  if (!props.oldForm || !props.newForm) return false;
  return (
    props.oldForm.website !== props.newForm.website
    || props.oldForm.github !== props.newForm.github
    || props.oldForm.twitter !== props.newForm.twitter
  );
};
</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectAddDetailsTab',
};
</script>
