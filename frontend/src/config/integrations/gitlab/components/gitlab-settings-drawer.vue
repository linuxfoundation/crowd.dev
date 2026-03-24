<template>
  <app-drawer
    v-model="isDrawerVisible"
    title="GitLab"
    size="600px"
    pre-title="Integration"
    :show-footer="true"
    has-border
    close-on-click-modal="true"
    :close-function="canClose"
    @close="isDrawerVisible = false"
  >
    <template #beforeTitle>
      <img :src="gitlabDetails.image" class="min-w-6 h-6 mr-2" alt="GitLab logo" />
    </template>
    <template #belowTitle>
      <drawer-description integration-key="gitlab" />
    </template>
    <template #content>
      <div class="flex gap-2 bg-brand-50 -mt-5 -mx-6 py-3 px-6 mb-5">
        <div>
          <lf-icon name="circle-info" type="solid" class="text-brand-500" :size="20" />
        </div>
        <div class="text-xs text-brand-800">
          Connected repositories are also synced through Git, which automatically mirrors your
          GitLab settings for adding, updating, and deleting repositories.
        </div>
      </div>
      <div>
        <!-- Connected user info -->
        <section v-if="connectedUser" class="border border-gray-200 rounded-md py-4 px-5 mb-6">
          <p class="text-2xs font-medium text-gray-400 mb-1">
            Connected account
          </p>
          <div class="flex items-center">
            <div v-if="connectedUser.avatar_url" class="h-5 w-5 rounded border border-gray-200 mr-2">
              <img :src="connectedUser.avatar_url" class="min-w-5 object-cover" :alt="connectedUser.name" />
            </div>
            <p class="text-xs font-medium leading-5">
              {{ connectedUser.name }} ({{ connectedUser.username }})
            </p>
          </div>
          <div class="mt-2">
            <p class="text-2xs font-medium text-gray-400 mb-1">
              Connected groups
            </p>
            <div v-if="connectedGroups.length > 0" class="flex flex-wrap gap-2">
              <lf-tag v-for="group in connectedGroups" :key="group.id" size="small" color="white">
                {{ group.name }}
              </lf-tag>
            </div>
            <p v-else class="text-xs text-gray-500">
              --
            </p>
          </div>
        </section>

        <!-- Disclaimer -->
        <section class="pb-4">
          <div class="pb-4">
            <div class="flex justify-between items-center">
              <h6 class="text-sm font-medium leading-5 mb-2">
                Repository mapping
              </h6>
              <div
                class="flex items-center text-primary-500 cursor-pointer select-none"
                @click="isBulkSelectOpened = true"
              >
                <lf-icon name="check-double" :size="16" class="mr-1" />
                <span class="text-xs font-normal">
                  Bulk selection
                </span>
              </div>
            </div>
            <p class="text-2xs leading-4.5 text-gray-500">
              Select the subproject you want to map with each connected
              repository, or choose to ignore it from sync.
            </p>
          </div>
          <div class="border border-yellow-100 rounded-md bg-yellow-50 p-2 flex">
            <lf-icon name="triangle-exclamation" type="solid" :size="16" class="text-yellow-500" />
            <div class="flex-grow text-yellow-900 text-2xs leading-4.5 pl-2">
              Repository mapping is not reversible. Once GitLab is connected,
              you won't be able to update these settings and reconnecting a
              different organization or repositories won't override past
              activities.
            </div>
          </div>
        </section>

        <section class="pb-4">
          <el-input v-model="search" clearable placeholder="Search repositories...">
            <template #prefix>
              <lf-icon name="magnifying-glass" class="text-gray-400" />
            </template>
          </el-input>
        </section>

        <!-- Repository mapping -->
        <section v-if="filteredProjects.length > 0">
          <div class="flex border-b border-gray-200 items-center h-8">
            <div class="w-8" />
            <div class="flex-grow pr-4">
              <p class="text-3xs uppercase text-gray-400 font-semibold tracking-1">
                REPOSITORY
              </p>
            </div>
            <div class="w-1/2 pr-4">
              <p class="text-3xs uppercase text-gray-400 font-semibold tracking-1">
                SUB-PROJECT
              </p>
            </div>
          </div>
          <div class="py-1.5">
            <template v-for="(projects, owner) in groupedFilteredProjects" :key="owner">
              <div class="bg-gray-50 py-2 px-2 flex items-center border-b border-t border-gray-200">
                <div v-if="getOwnerAvatar(owner)" class="w-5 h-5 rounded-full mr-2 overflow-hidden">
                  <img :src="getOwnerAvatar(owner)" class="w-full h-full object-cover" alt="Owner avatar" />
                </div>
                <lf-icon v-else name="house-building" :size="20" />
                <span class="text-xs font-medium">{{ owner }}</span>
              </div>
              <article v-for="project in projects" :key="project.id" class="py-1.5 flex items-center">
                <div class="w-8 flex items-center justify-center">
                  <lf-checkbox v-model="selectedRepos[project.web_url]" size="large" @change="updateSelectedRepos" />
                </div>
                <div class="flex-grow flex items-center pr-4">
                  <lf-svg name="git-repository" class="w-4 h-4 mr-2" />
                  <p class="text-2xs leading-5 flex-grow truncate">
                    {{ project.path_with_namespace }}
                  </p>
                </div>
                <div class="w-1/2 flex items-center">
                  <app-form-item
                    :validation="$v[project.web_url]"
                    :error-messages="{
                      required: 'This field is required',
                    }"
                    class="mb-0 flex-grow"
                    error-class="relative top-0"
                  >
                    <el-select
                      v-model="form[project.web_url]"
                      placeholder="Select sub-project"
                      class="w-full"
                      placement="bottom-end"
                      filterable
                      @blur="$v[project.web_url].$touch"
                      @change="$v[project.web_url].$touch"
                    >
                      <el-option
                        v-for="subproject in subprojects"
                        :key="subproject.id"
                        :value="subproject.id"
                        :label="subproject.name"
                      />
                    </el-select>
                  </app-form-item>
                </div>
              </article>
            </template>
          </div>
        </section>
        <section v-else>
          <p class="text-center text-sm text-gray-500 mb-4">
            No repositories found
          </p>
        </section>
      </div>
    </template>

    <template #footer>
      <drawer-footer-buttons
        :integration="props.integration"
        :is-edit-mode="!!props.integration"
        :has-form-changed="hasFormChanged"
        :is-loading="false"
        :is-submit-disabled="sending || $v.$invalid || !hasSelectedRepos"
        :cancel="() => (isDrawerVisible = false)"
        :revert-changes="revertChanges"
        :connect="connect"
      />
    </template>
  </app-drawer>
  <app-gitlab-settings-bulk-select
    v-model="isBulkSelectOpened"
    :repositories="groupedProjects"
    :subprojects="subprojects"
    :mapped-repos="form"
    @apply="bulkApply"
  />
  <changes-confirmation-modal ref="changesConfirmationModalRef" />
</template>

<script lang="ts" setup>
import {
  computed, onMounted, ref, watch,
} from 'vue';
import { ToastStore } from '@/shared/message/notification';
import gitlab from '@/config/integrations/gitlab/config';
import { LfService } from '@/modules/lf/segments/lf-segments-service';
import { useRouter } from 'vue-router';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';
import AppFormItem from '@/shared/form/form-item.vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import ConfirmDialog from '@/shared/dialog/confirm-dialog';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import { showIntegrationProgressNotification } from '@/modules/integration/helpers/integration-progress-notification';
import LfSvg from '@/shared/svg/svg.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfCheckbox from '@/ui-kit/checkbox/Checkbox.vue';
import LfTag from '@/ui-kit/tag/Tag.vue';
import DrawerDescription from '@/modules/admin/modules/integration/components/drawer-description.vue';
import DrawerFooterButtons from '@/modules/admin/modules/integration/components/drawer-footer-buttons.vue';
import ChangesConfirmationModal from '@/modules/admin/modules/integration/components/changes-confirmation-modal.vue';
import AppGitlabSettingsBulkSelect from './gitlab-settings-bulk-select.vue';

const props = defineProps<{
  modelValue: boolean;
  integration: any;
  segmentId: string | null;
  grandparentId: string | null;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void }>();

const router = useRouter();
const changesConfirmationModalRef = ref<InstanceType<typeof ChangesConfirmationModal> | null>(null);

// Store
const { doFetch } = mapActions('integration');

// Drawer visibility
const isDrawerVisible = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emit('update:modelValue', val);
  },
});

// Search
const search = ref('');

// Connected user
const connectedUser = computed(() => props.integration?.settings?.user || null);

// Connected groups
const connectedGroups = computed(
  () => props.integration?.settings?.groups || [],
);

const pathToFullURL = (path: string) => `https://gitlab.com/${path}`;

// All projects (user projects + group projects)
const allProjects = computed(() => {
  const userProjects = props.integration?.settings?.userProjects || [];
  const groupProjects = Object.values(
    props.integration?.settings?.groupProjects || {},
  ).flat();
  const projects = [...userProjects, ...groupProjects].map((project) => ({
    ...project,
    web_url: pathToFullURL(project.path_with_namespace),
  }));
  return projects;
});

// eslint-disable-next-line vue/max-len
const filteredProjects = computed(() => allProjects.value.filter((p: any) => p.path_with_namespace.toLowerCase().includes(search.value.toLowerCase())));

// Group filtered projects by owner
const groupedFilteredProjects = computed(() => filteredProjects.value.reduce((acc, project) => {
  const owner = project.groupName || connectedUser.value.name;
  if (!acc[owner]) {
    acc[owner] = [];
  }
  acc[owner].push(project);
  return acc;
}, {}));

const groupedProjects = computed(() => allProjects.value.reduce((acc, project) => {
  const owner = project.groupName || connectedUser.value.name;
  if (!acc[owner]) {
    acc[owner] = [];
  }
  acc[owner].push(project);
  return acc;
}, {}));

// Static gitlab details
const gitlabDetails = gitlab;

// Form
const form = ref<Record<string, string>>(
  allProjects.value.reduce(
    (a: Record<string, any>, b: any) => ({
      ...a,
      [b.web_url]: props.integration.segmentId,
    }),
    {},
  ),
);

const hasFormChanged = computed(() => {
  const original = allProjects.value.reduce(
    (a: Record<string, any>, b: any) => ({
      ...a,
      [b.url]: props.integration.segmentId,
    }),
    {},
  );
  return JSON.stringify(form.value) !== JSON.stringify(original);
});

const rules = computed(() => allProjects.value.reduce(
  (a: Record<string, any>, b: any) => ({
    ...a,
    [b.web_url]: {
      required,
    },
  }),
  {},
));

const $v = useVuelidate(rules, form);

// Connecting
const sending = ref(false);

// Track selected repositories
const selectedRepos = ref<Record<string, boolean>>({});

// Initialize selectedRepos with current segment/subproject as default
onMounted(() => {
  allProjects.value.forEach((project) => {
    selectedRepos.value[project.web_url] = false;
    form.value[project.web_url] = props.integration.segmentId;
  });
});

const revertChanges = () => {
  allProjects.value.forEach((project) => {
    selectedRepos.value[project.web_url] = false;
    form.value[project.web_url] = props.integration.segmentId;
  });
};

const canClose = (done: (value: boolean) => void) => {
  if (hasFormChanged.value) {
    changesConfirmationModalRef.value?.open().then((discardChanges: boolean) => {
      if (discardChanges) {
        revertChanges();
        done(false);
      } else {
        done(true);
      }
    });
  } else {
    done(false);
  }
};

// Update selected repos
const updateSelectedRepos = () => {
  // This function will be called whenever a checkbox is toggled
};

// Check if any repos are selected
const hasSelectedRepos = computed(() => Object.values(selectedRepos.value).some((value) => value === true));

// Get avatar for owner (user or group)
const getOwnerAvatar = (owner: string | number) => {
  if (owner === connectedUser.value.name) {
    return connectedUser.value.avatar_url;
  }
  const group = connectedGroups.value.find(
    (g: { name: string }) => g.name === owner,
  );
  return group?.avatarUrl;
};

// Update connect function to only send selected repositories
const connect = () => {
  const selectedData = Object.entries(form.value).reduce<
    Record<string, string>
  >((acc, [key, value]) => {
    if (selectedRepos.value[key]) {
      acc[key] = value;
    }
    return acc;
  }, {});

  ConfirmDialog({
    type: 'warning',
    title: 'Are you sure you want to proceed?',
    message:
      "Repository mapping is not reversible. Once GitLab is connected, you won't be able to update these settings.\n\n"
      + "Reconnecting a different organization and/or repositories won't remove past activities. "
      + 'In order to clean up existing data please reach out to our support team.',
    confirmButtonText: 'Connect GitLab',
    cancelButtonText: 'Cancel',
    icon: 'fa-triangle-exclamation fa-solid',
  } as any).then(() => {
    IntegrationService.mapGitlabRepos(
      props.integration.id,
      selectedData,
      allProjects.value
        .filter((p) => selectedRepos.value[p.web_url])
        .map((p) => p.id),
      [props.integration.segmentId],
    )
      .then(() => {
        isDrawerVisible.value = false;

        doFetch([props.integration.segmentId]);

        showIntegrationProgressNotification(
          'gitlab',
          props.integration.segmentId,
        );

        router.push({
          name: 'integration',
          params: {
            id: props.integration.segmentId,
          },
        });
      })
      .catch((e) => {
        ToastStore.error('There was an error mapping gitlab repos');
        console.error(e);
      });
  });
};

// Bulk select
const isBulkSelectOpened = ref<boolean>(false);

const bulkApply = (data: Record<string, string>) => {
  Object.entries(data).forEach(([repoUrl, segmentId]) => {
    form.value[repoUrl] = segmentId;
    selectedRepos.value[repoUrl] = true;
  });

  // Force reactivity update
  selectedRepos.value = { ...selectedRepos.value };

  // Check if all repositories are selected
  if (Object.keys(data).length === allProjects.value.length) {
    allProjects.value.forEach((project) => {
      selectedRepos.value[project.web_url] = true;
    });
  }
};

// Fetching subprojects
const subprojects = ref([]);

const fetchSubProjects = () => {
  LfService.findSegment(props.grandparentId).then((segment: any) => {
    subprojects.value = segment.projects.map((p: any) => p.subprojects).flat();
  });
};

onMounted(() => {
  fetchSubProjects();
});

watch(form, (newForm) => {
  Object.keys(newForm).forEach((repoUrl) => {
    if (newForm[repoUrl]) {
      selectedRepos.value[repoUrl] = true;
    }
  });
  // Force reactivity update
  selectedRepos.value = { ...selectedRepos.value };
}, { deep: true });
</script>

<script lang="ts">
export default {
  name: 'AppGitlabSettingsDrawer',
};
</script>
