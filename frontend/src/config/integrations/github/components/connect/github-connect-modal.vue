<template>
  <lf-modal v-model="isModalOpen" width="40rem">
    <div class="p-6">
      <div class="flex justify-between">
        <!-- icon -->
        <div class="bg-primary-50 h-10 w-10 flex justify-center items-center rounded-full">
          <lf-svg name="git-repository-private" class="w-6 h-6 text-primary-500" />
        </div>

        <!-- close button -->
        <lf-button
          type="secondary-ghost-light"
          size="large"
          icon-only
          class="-mt-2 -mr-2"
          @click="isModalOpen = false"
        >
          <lf-icon name="xmark" type="regular" :size="24" />
        </lf-button>
      </div>
      <section class="pt-6 pb-8">
        <h5 class="pb-2">
          Connect your GitHub organization
        </h5>
        <p class="text-medium text-gray-600">
          Choose the organization and repositories you want to connect with your project.
        </p>
      </section>
      <section class="mb-7 border border-gray-200 rounded-lg p-5">
        <h6 class="pb-2">
          I'm the GitHub organization admin
        </h6>
        <p class="text-medium text-gray-600 pb-6">
          As an admin member of the organization you want to connect,
          you can give access to Community Data Platform and select the repositories you want to track.
        </p>
        <lf-button type="primary" class="w-full" @click="openGithubInstallation">
          Choose organization to connect
        </lf-button>
      </section>
      <section class="border border-gray-200 rounded-lg p-5">
        <article class="mb-8">
          <h6 class="pb-2">
            I'm not the admin of the GitHub organization
          </h6>
          <p class="text-medium text-gray-600">
            Request the installation of Community Data Platform App from an admin member.
          </p>
        </article>
        <article class="flex mb-8">
          <div class="min-w-8 w-8 h-8 flex items-center justify-center rounded-full bg-gray-200">
            <h6>1</h6>
          </div>
          <div class="pl-3">
            <p class="text-medium text-gray-600 pb-3">
              Share the <span class="font-semibold">Community Data Platform App</span> installation link
              with an admin member of your GitHub organization.
            </p>
            <lf-button type="primary-link" size="small" @click="copy()">
              <template v-if="!copied">
                <lf-icon name="copy" />
                Copy app installation link to clipboard
              </template>
              <template v-else>
                <lf-icon name="circle-check" class="text-green-500" />
                <span class="text-green-500">Copied to clipboard!</span>
              </template>
            </lf-button>
          </div>
        </article>
        <article class="flex mb-6">
          <div class="min-w-8 w-8 h-8 flex items-center justify-center rounded-full bg-gray-200">
            <h6>2</h6>
          </div>
          <div class="pl-3">
            <p class="text-medium text-gray-600 pb-3">
              Once the app is installed, you will be able to select the organization you want to connect.
              Type the organization name in order to proceed.
            </p>
          </div>
        </article>
        <article>
          <el-select
            v-model="installationId"
            placeholder="Enter organization name..."
            class="w-full mb-4"
            filterable
            :filter-method="(query: string) => search = query"
            no-data-text="Type to search"
            clearable
            :automatic-dropdown="false"
            :popper-class="search.length ? '' : 'hidden'"
          >
            <template v-if="selectedInstallation" #prefix>
              <lf-avatar
                :src="selectedInstallation.avatarUrl"
                :name="selectedInstallation.login"
                :size="18"
                class="!rounded border border-gray-200 mr-1 mt-px"
              >
                <template #placeholder>
                  <div class="w-full h-full bg-gray-50 flex items-center justify-center">
                    <lf-icon name="house-building" :size="14" class="text-gray-400" />
                  </div>
                </template>
              </lf-avatar>
            </template>
            <el-option
              v-for="i of filteredInstallations"
              :key="i.installationId"
              :value="i.installationId"
              :label="i.login"
              class="!px-3"
            >
              <div class="flex items-center gap-2">
                <lf-avatar :src="i.avatarUrl" :name="i.login" :size="18" class="!rounded border border-gray-200 mr-1 mt-px">
                  <template #placeholder>
                    <div class="w-full h-full bg-gray-50 flex items-center justify-center">
                      <lf-icon name="house-building" :size="14" class="text-gray-400" />
                    </div>
                  </template>
                </lf-avatar>
                <p>{{ i.login }}</p>
              </div>
            </el-option>
          </el-select>

          <lf-button class="w-full" :disabled="!installationId.length" @click="connectInstallation">
            Connect organization
          </lf-button>
        </article>
      </section>
    </div>
  </lf-modal>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import config from '@/config';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfAvatar from '@/ui-kit/avatar/Avatar.vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import { mapActions } from '@/shared/vuex/vuex.helpers';
import LfSvg from '@/shared/svg/svg.vue';

interface GithubInstallation {
  id: string;
  installationId: string;
  type: string;
  numRepos: number;
  login: string;
  avatarUrl: string;
}

const props = defineProps<{
  modelValue: boolean,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void }>();

const { doFetch } = mapActions('integration');
const copied = ref(false);
const installations = ref<GithubInstallation[]>([]);
const installationId = ref<string>('');
const search = ref<string>('');

const isModalOpen = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
});

const filteredInstallations = computed(() => (search.value.length
  ? installations.value.filter((i) => i.login.toLowerCase().startsWith(search.value.toLowerCase()))
  : []));

const selectedInstallation = computed(() => installations.value.find((i) => i.installationId === installationId.value));

const openGithubInstallation = () => window.open(config.gitHubInstallationUrl, '_self');

const copy = () => {
  if (navigator.clipboard) {
    const urlWithState = `${config.gitHubInstallationUrl}?state=noconnect`;
    navigator.clipboard.writeText(urlWithState);
    copied.value = true;
    setTimeout(() => {
      copied.value = false;
    }, 1000);
  }
};

const connectInstallation = () => {
  if (!installationId.value) {
    return;
  }

  IntegrationService.githubConnectInstallation(installationId.value)
    .then(() => {
      isModalOpen.value = false;
      doFetch();
    });
};

const getGithubInstallations = () => {
  IntegrationService.getGithubInstallations()
    .then((res: GithubInstallation[]) => {
      installations.value = res;
    });
};

onMounted(() => {
  getGithubInstallations();
});
</script>

<script lang="ts">
export default {
  name: 'LfGithubConnectModal',
};
</script>
