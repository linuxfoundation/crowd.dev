<template>
  <div>
    <section
      v-for="[key, value] in Object.entries(identitiesForm)"
      :key="key"
      class="border-b border-gray-200 last:border-none pt-5 pb-6"
    >
      <div v-if="lfIdentities[key]" class="flex">
        <div class="w-6 pt-2 mr-4">
          <img
            :src="lfIdentities[key].image"
            :alt="lfIdentities[key].name"
            class="min-w-6"
          />
        </div>
        <div class="flex-grow">
          <template v-for="(identity, ii) of model" :key="ii">
            <article
              v-if="identity.platform === key"
              class="flex flex-grow gap-2 pb-3 last:pb-0"
            >
              <el-input
                v-model="model[ii].value"
                :placeholder="
                  identity.value.length ? identity.value : 'johndoe'
                "
              >
                <template #prepend>
                  <span class="font-medium text-gray-500">{{
                    value.urlPrefix
                  }}</span>
                </template>
              </el-input>

              <el-tooltip
                v-if="
                  props.showUnmerge && Object.entries(identitiesForm).length > 1
                "
                :disabled="
                  !staticModel?.[ii]?.value
                    || staticModel?.[ii]?.value === model[ii].value
                "
                content="Not possible to unmerge an unsaved identity"
                placement="top"
              >
                <div>
                  <lf-button
                    type="secondary-ghost"
                    size="medium"
                    class="block w-8 !h-8 p-0"
                    :disabled="
                      !staticModel?.[ii]?.value
                        || staticModel?.[ii]?.value !== model[ii].value
                    "
                    @click="
                      emit('unmerge', {
                        platform: key,
                        value: staticModel?.[ii]?.value,
                      })
                    "
                  >
                    <lf-icon name="link-slash" :size="20" />
                  </lf-button>
                </div>
              </el-tooltip>
              <lf-button
                :disabled="editingDisabled(key)"
                type="secondary-ghost"
                size="medium"
                class="w-8 !h-8"
                @click="removeUsername(ii)"
              >
                <lf-icon name="trash-can" :size="20" />
              </lf-button>
            </article>
          </template>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import {
  computed, ref, watch, reactive,
} from 'vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { lfIdentities } from '@/config/identities';
import LfButton from '@/ui-kit/button/Button.vue';
import type { Platform } from '@/shared/modules/platform/types/Platform';
import type { Organization, OrganizationIdentity } from '../../types/Organization';
import { OrganizationIdentityType } from '../../types/Organization';

defineOptions({
  name: 'AppOrganizationFormIdentities',
});

const props = withDefaults(defineProps<{
  modelValue: Organization;
  record: Organization | null;
  showUnmerge?: boolean;
}>(), {
  showUnmerge: false,
});

const emit = defineEmits<{(e: 'update:modelValue', value: Organization): void;
  (e: 'unmerge', value: { platform: string; value?: string }): void;
}>();

// TODO: move this to identities config
const identitiesForm = {
  github: {
    urlPrefix: 'github.com/',
  },
  linkedin: {
    urlPrefix: 'linkedin.com/company/',
  },
  twitter: {
    urlPrefix: 'twitter.com/',
  },
  crunchbase: {
    urlPrefix: 'crunchbase.com/organization/',
  },
};

const model = ref<OrganizationIdentity[]>([]);

watch(
  props.modelValue,
  (organization, previous) => {
    if (!previous) {
      const { identities } = organization;
      const platforms = [
        ...new Set(organization.identities.map((i) => i.platform)),
      ];
      const noIdentity = (Object.keys(identitiesForm) as Platform[])
        .filter((platform) => !platforms.includes(platform))
        .map((platform) => reactive({
          value: '',
          type: OrganizationIdentityType.USERNAME,
          verified: true,
          platform,
          source: 'ui',
        }));

      model.value = [...identities, ...noIdentity];
    }
  },
  { deep: true, immediate: true },
);

watch(
  model,
  (value) => {
    // Parse username object

    const identities = value.filter(
      (i) => !Object.keys(identitiesForm).includes(i.platform)
        || !!i.value?.trim().length,
    );

    // Emit updated member
    emit('update:modelValue', {
      ...props.modelValue,
      identities,
    });
  },
  { deep: true },
);

function editingDisabled(platform: string) {
  return model.value.filter((i) => i.platform === platform).length < 2;
}

const removeUsername = (index: number) => {
  if (model.value.length > 1) {
    model.value.splice(index, 1);
  }
};

const staticModel = computed(() => props.record?.identities);
</script>
