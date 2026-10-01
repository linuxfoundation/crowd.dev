<template>
  <el-popover v-model:visible="visible" placement="bottom-start" trigger="click" popper-class="!p-0" width="20rem">
    <template #reference>
      <lf-button type="secondary" size="small">
        <lf-icon name="layer-group" /> <p>Projects: <span class="font-normal">{{ label }}</span></p>
      </lf-button>
    </template>

    <div class="pt-1.5 pb-2 px-2">
      <article
        class="px-3 py-2.5 leading-5 font-xs flex justify-between items-center transition cursor-pointer rounded-md hover:bg-gray-50"
        :class="empty ? '!bg-primary-50' : ''"
        @click="segments = []; childSegments = []"
      >
        <span class="text-black">All projects</span>
        <lf-icon v-if="empty" name="check" :size="20" class="text-primary-600" />
      </article>
    </div>
    <div class="border-t border-gray-100 px-2 pt-2 pb-1 w-full sticky top-0 bg-white z-10">
      <el-input
        id="filterSearch"
        v-model="searchQuery"
        placeholder="Search for projects"
        class="lf-filter-input filter-dropdown-search"
        data-qa="filter-list-search"
        @input="onSearchQueryChange"
      />
    </div>
    <div class="p-2 border-t border-gray-100 flex flex-col gap-1 max-h-72 overflow-auto">
      <section v-for="project of projects.list" :key="project.id">
        <label
          class="px-3 py-2.5 leading-5 font-xs flex items-center transition cursor-pointer rounded-md hover:bg-gray-50"
        >
          <lf-checkbox
            v-model="segments"
            :value="project.id"
            class="!mr-0"
            @update:model-value="onProjectSelect(project)"
          />
          <p class="text-black text-xs !break-normal">
            {{ project.name }}
          </p>
        </label>
        <label
          v-for="subproject of project.subprojects"
          :key="subproject.id"
          class="pr-3 pl-10 py-2.5 leading-5 font-xs flex items-center transition cursor-pointer rounded-md hover:bg-gray-50"
        >
          <lf-checkbox
            v-model="childSegments"
            :value="subproject.id"
            class="!mr-0"
            @update:model-value="onSubprojectSelect(project, subproject)"
          />
          <p class="text-black text-xs !break-normal">
            {{ subproject.name }}
          </p>
        </label>
      </section>
    </div>
    <div class="py-3 px-4 border-t border-gray-100 flex justify-end gap-3">
      <lf-button size="small" type="secondary-ghost" @click="visible = false">
        Cancel
      </lf-button>
      <lf-button size="small" type="primary" :disabled="!hasChanged" @click="apply()">
        Apply
      </lf-button>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import {
  computed, onMounted, ref,
} from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import isEqual from 'lodash/isEqual';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import { EventType, FeatureEventKey } from '@/shared/modules/monitoring/types/event';
import { useRouter } from 'vue-router';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps<{
  segments: string[]
  childSegments: string[]
}>();

const emit = defineEmits<{(e: 'update:segments', value: string[]): void,
  (e: 'update:childSegments', value: string[]): void}>();

const visible = ref<boolean>(false);

const segments = ref<string[]>(props.segments);
const childSegments = ref<string[]>(props.childSegments);

const { trackEvent } = useProductTracking();
const router = useRouter();

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup, projects } = storeToRefs(lsSegmentsStore);
const { listProjects } = lsSegmentsStore;

const labelData: Record<string, {
  name: string;
  parentId?: string;
  parentName?: string;
}> = {};

const label = computed(() => {
  if (props.segments.length === 0 && props.childSegments.length === 0) {
    return 'All';
  }
  const labels: string[] = [];
  props.segments.forEach((s) => {
    labels.push(`${labelData[s]?.name || ''} (all sub-projects)`);
  });

  props.childSegments
    .filter((cs) => !props.segments.includes(labelData[cs]?.parentId || ''))
    .forEach((s) => {
      labels.push(`${labelData[s]?.name || ''}`);
    });

  const joined = labels.join(', ');
  return joined.length > 40 ? `${joined.substring(0, 40)}...` : joined;
});

const loadProjects = (search: string) => {
  listProjects({
    parentSlug: selectedProjectGroup.value.slug, search, reset: true, limit: 40,
  });
};

// Search
const searchQuery = ref('');
const onSearchQueryChange = (value: string) => {
  setTimeout(() => {
    if (value === searchQuery.value) {
      loadProjects(value);
    }
  }, 300);
};

const empty = computed(() => (segments.value.length + childSegments.value.length) === 0);

const onProjectSelect = (project: any) => {
  labelData[project.id] = {
    name: project.name,
  };
  const subprojectIds = project.subprojects.map((sp: any) => sp.id);
  if (segments.value.includes(project.id)) {
    childSegments.value = [...new Set([
      ...childSegments.value,
      ...subprojectIds,
    ])];
  } else {
    childSegments.value = childSegments.value.filter((cs) => !subprojectIds.includes(cs));
  }
  project.subprojects.forEach((sp) => {
    labelData[sp.id] = {
      name: sp.name,
      parentId: project.id,
      parentName: project.name,
    };
  });
};

const onSubprojectSelect = (project: any, subproject: any) => {
  labelData[subproject.id] = {
    name: subproject.name,
    parentId: project.id,
    parentName: project.name,
  };
  labelData[project.id] = {
    name: project.name,
  };
  const subprojectIds = project.subprojects.map((sp: any) => sp.id);
  const allSubsChecked = subprojectIds.every((spi) => childSegments.value.includes(spi));
  if (allSubsChecked) {
    segments.value = [...new Set([
      ...segments.value,
      project.id,
    ])];
  } else {
    segments.value = segments.value.filter((s) => s !== project.id);
  }
};

const apply = () => {
  emit('update:segments', segments.value);
  emit('update:childSegments', childSegments.value);

  let key: FeatureEventKey | null = null;
  const { name: routeName } = router.currentRoute.value;

  if (routeName === 'memberMergeSuggestions') {
    key = FeatureEventKey.FILTER_MEMBERS_MERGE_SUGGESTIONS;
  } else if (routeName === 'organizationMergeSuggestions') {
    key = FeatureEventKey.FILTER_ORGANIZATIONS_MERGE_SUGGESTIONS;
  } else {
    key = FeatureEventKey.FILTER;
  }

  if (key) {
    trackEvent({
      key,
      type: EventType.FEATURE,
      properties: {
        filter: {
          projects: segments.value,
          subprojects: childSegments.value,
        },
      },
    });
  }

  visible.value = false;
};

const hasChanged = computed(() => !isEqual(segments.value, props.segments) || !isEqual(childSegments.value, props.childSegments));

onMounted(() => {
  loadProjects('');
});
</script>

<script lang="ts">
export default {
  name: 'AppMergeSuggestionsProjectsFilter',
};
</script>
