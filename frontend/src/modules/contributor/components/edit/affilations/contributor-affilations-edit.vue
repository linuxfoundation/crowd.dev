<template>
  <lf-modal v-model="isModalOpen" width="60rem">
    <section class="px-6 pt-4 sticky top-0 z-10 bg-white">
      <div class="flex items-center justify-between pb-1.5">
        <h5>
          Activities affiliation
        </h5>
        <lf-button type="secondary-ghost-light" icon-only @click="isModalOpen = false">
          <lf-icon name="xmark" />
        </lf-button>
      </div>
      <p class="text-gray-500 text-medium">
        Manage the affiliation between activities and organizations on behalf of a project over a specific time
        period.<br>
        By default, activities are affiliated with organizations based on the duration of the work experience.<br>
        <br>
        <span class="font-semibold">Important note:</span> Work history updates won’t override manual changes to
        activities affiliations.
      </p>
      <div class="flex border-b border-gray-100 pt-6">
        <div class="w-1/3 py-2">
          <p class="text-medium font-semibold text-gray-400">
            Project
          </p>
        </div>
        <div class="w-2/3 py-2">
          <p class="text-medium font-semibold text-gray-400">
            Affiliation & period
          </p>
        </div>
      </div>
    </section>
    <section class="px-6">
      <div>
        <div class="pb-10">
          <article
            v-for="subproject of props.contributor.segments"
            :key="subproject.id"
            class="flex border-t border-gray-100 first:border-none py-4"
          >
            <div class="w-1/3 h-10">
              <p class="text-medium font-semibold py-2.5">
                {{ subproject.name }}
              </p>
            </div>
            <div class="w-2/3 py-2">
              <div class="flex flex-col gap-4 items-start">
                <template
                  v-for="(affiliation, ai) of form"
                  :key="`${subproject.id}-${ai}`"
                >
                  <template v-if="affiliation.segmentId === subproject.id">
                    <lf-contributor-edit-affilations-item
                      v-model="form[ai]"
                      :contributor="props.contributor"
                    >
                      <lf-dropdown placement="bottom-end" width="14rem">
                        <template #trigger>
                          <lf-button
                            type="secondary-ghost"
                            class="ml-2 my-1"
                            icon-only
                          >
                            <lf-icon name="ellipsis" type="regular" />
                          </lf-button>
                        </template>
                        <lf-dropdown-item
                          @click="copyToOtherProjects(ai)"
                        >
                          <lf-icon name="copy" type="regular" /> Apply to all projects
                        </lf-dropdown-item>
                        <lf-dropdown-item
                          type="danger"
                          @click="form.splice(ai, 1)"
                        >
                          <lf-icon name="trash-can" /> Delete affiliation
                        </lf-dropdown-item>
                      </lf-dropdown>
                    </lf-contributor-edit-affilations-item>
                  </template>
                </template>

                <lf-button
                  type="primary-link"
                  size="small"
                  :disabled="isProjectInvalid(subproject.id)"
                  class="mt-1"
                  @click="addAffiliation(subproject.id)"
                >
                  <lf-icon name="plus" />
                  Add affiliation
                </lf-button>
              </div>
            </div>
          </article>
        </div>
      </div>
    </section>
    <footer class="border-t border-gray-100 px-6 py-4 flex justify-end gap-4 sticky bottom-0 bg-white">
      <lf-button type="secondary-ghost" @click="isModalOpen = false">
        Cancel
      </lf-button>
      <lf-button type="primary" :disabled="$v.$invalid || !hasFormChanged" @click="submit()">
        Update activities affiliation
      </lf-button>
    </footer>
  </lf-modal>
</template>

<script setup lang="ts">
import LfModal from '@/ui-kit/modal/Modal.vue';
import { useContributorStore } from '@/modules/contributor/store/contributor.store';
import type { Contributor, ContributorAffiliation } from '@/modules/contributor/types/Contributor';
import { computed, onMounted, ref } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import type { AffilationForm } from '@/modules/contributor/components/edit/affilations/contributor-affilations-edit-item.vue';
import LfContributorEditAffilationsItem
  from '@/modules/contributor/components/edit/affilations/contributor-affilations-edit-item.vue';
import useVuelidate from '@vuelidate/core';

import { ToastStore } from '@/shared/message/notification';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  modelValue: boolean,
  contributor: Contributor,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void }>();

const { updateContributorAffiliations } = useContributorStore();

const isModalOpen = computed<boolean>({
  get() {
    return props.modelValue;
  },
  set(value: boolean) {
    emit('update:modelValue', value);
  },
});

const form = ref<AffilationForm[]>([]);
const initialForm = ref<AffilationForm[]>([]);

const $v = useVuelidate({}, form);

const addAffiliation = (subprojectId: string) => {
  form.value.push({
    segmentId: subprojectId,
    organization: null,
    dateStart: '',
    dateEnd: '',
    currentlyAffiliated: false,
  });
};

const sending = ref<boolean>(false);

const submit = () => {
  if ($v.value.$invalid) {
    return;
  }
  const affiliations: Partial<ContributorAffiliation>[] = form.value.map((affiliation) => ({
    memberId: props.contributor.id,
    segmentId: affiliation.segmentId,
    organizationId: affiliation.organization,
    dateStart: affiliation.dateStart
      ? dateHelper(affiliation.dateStart).startOf('month').format('YYYY-MM-DDTHH:mm:ss.SSS[Z]')
      : undefined,
    dateEnd: !affiliation.currentlyAffiliated && affiliation.dateEnd
      ? dateHelper(affiliation.dateEnd).startOf('month').format('YYYY-MM-DDTHH:mm:ss.SSS[Z]')
      : undefined,
  }));

  sending.value = true;

  updateContributorAffiliations(props.contributor.id, affiliations)
    .then(() => {
      ToastStore.success('Activities affiliation updated successfully');
      isModalOpen.value = false;
    })
    .catch(() => {
      ToastStore.error('Failed to update activities affiliation');
    })
    .finally(() => {
      sending.value = false;
    });
};

const isProjectInvalid = (projectId: string) => form.value.some((affiliation) => affiliation.segmentId === projectId
  && (!affiliation.organization || !affiliation.dateStart || (!affiliation.currentlyAffiliated && !affiliation.dateEnd)));

const hasFormChanged = computed(() => JSON.stringify(form.value) !== JSON.stringify(initialForm.value));

const copyToOtherProjects = (index: number) => {
  const affiliation = form.value[index];
  const otherProjects = props.contributor.segments.filter((seg) => seg.id !== affiliation.segmentId);

  otherProjects.forEach((project) => {
    form.value.push({
      segmentId: project.id,
      organization: affiliation.organization,
      dateStart: affiliation.dateStart,
      dateEnd: affiliation.dateEnd,
      currentlyAffiliated: affiliation.currentlyAffiliated,
    });
  });
};

onMounted(() => {
  form.value = props.contributor.affiliations.map((affiliation) => ({
    segmentId: affiliation.segmentId || '',
    organization: affiliation.organizationId || null,
    dateStart: affiliation.dateStart || '',
    dateEnd: affiliation.dateEnd || '',
    currentlyAffiliated: !affiliation.dateEnd && !!affiliation.dateStart,
  }));
  initialForm.value = [...form.value];
});
</script>

<script lang="ts">
export default {
  name: 'LfContributorEditAffilations',
};
</script>
