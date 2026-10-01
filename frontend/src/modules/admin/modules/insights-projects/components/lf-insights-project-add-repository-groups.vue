<template>
  <div>
    <div v-if="cForm.repositoryGroups.length > 0">
      <div class="py-2 px-6 bg-gray-50 border-b border-gray-100 -mx-6">
        <lf-button type="primary-ghost" size="small" @click="add()">
          <lf-icon name="plus" />
          Add repository group
        </lf-button>
      </div>
      <div class="py-3">
        <article
          v-for="(group, gi) of cForm.repositoryGroups"
          :key="gi"
          class="flex justify-between items-center border-t first:border-t-0 border-gray-100 py-2.5"
        >
          <p class="text-medium">
            {{ group.name }}
          </p>
          <div class="flex items-center gap-4">
            <lf-badge type="secondary">
              <div class="gap-1 flex items-center">
                <lf-svg
                  name="git-repository"
                  class="w-3.5 h-3.5"
                />
                {{ pluralize('repository', group.repositories.length, true) }}
              </div>
            </lf-badge>
            <div class="flex items-center gap-2">
              <lf-button icon-only type="secondary-ghost-light" @click="edit(gi)">
                <lf-icon name="edit" />
              </lf-button>
              <lf-button icon-only type="secondary-ghost-light" @click="remove(gi)">
                <lf-icon name="trash-can" />
              </lf-button>
            </div>
          </div>
        </article>
      </div>
    </div>
    <div v-else class="py-20 flex flex-col items-center">
      <lf-icon name="list-tree" :size="80" class="text-gray-300" />
      <h6 class="text-center text-h6 pt-6 pb-3">
        No repository groups yet
      </h6>
      <p class="text-small text-gray-500 text-center pb-6">
        Create the first group of repositories for this project
      </p>
      <lf-button type="primary-ghost" @click="add()">
        <lf-icon name="plus" />
        Add repository group
      </lf-button>
    </div>
  </div>
  <lf-repository-groups-modal
    v-if="isModalOpen"
    v-model="isModalOpen"
    :repositories="repositories"
    :repository-group="editIndex >= 0 ? cForm.repositoryGroups[editIndex] : null"
    @add="create"
    @edit="update"
  />
</template>

<script setup lang="ts">
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import { computed, reactive, ref } from 'vue';
import LfRepositoryGroupsModal
  from '@/modules/admin/modules/insights-projects/components/repository-groups/lf-repository-groups-modal.vue';
import LfBadge from '@/ui-kit/badge/Badge.vue';
import pluralize from 'pluralize';
import LfSvg from '@/shared/svg/svg.vue';
import type { InsightsProjectAddFormModel } from '../models/insights-project-add-form.model';

interface RepositoryGroup {
  id?: string;
  name: string;
  repositories: string[];
}

const props = defineProps<{
  form: InsightsProjectAddFormModel;
}>();

const cForm = reactive(props.form);

const isModalOpen = ref(false);
const editIndex = ref(-1);

const repositories = computed(() => props.form.repositories.filter((r) => r.enabled));

const add = () => {
  editIndex.value = -1;
  isModalOpen.value = true;
};
const edit = (index: number) => {
  editIndex.value = index;
  isModalOpen.value = true;
};

const create = (data: RepositoryGroup) => {
  cForm.repositoryGroups = [...cForm.repositoryGroups, data];
};

const update = (data: RepositoryGroup) => {
  const list = [...cForm.repositoryGroups];
  if (editIndex.value >= 0 && editIndex.value < list.length) {
    list[editIndex.value] = {
      ...list[editIndex.value],
      ...data,
    };
    cForm.repositoryGroups = list;
  }
};

const remove = (index: number) => {
  const list = [...cForm.repositoryGroups];
  if (index >= 0 && index < list.length) {
    list.splice(index, 1);
    cForm.repositoryGroups = list;
  }
};
</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectAddRepositoryGroups',
};
</script>
