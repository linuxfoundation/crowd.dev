<template>
  <section v-bind="$attrs">
    <div class="flex items-center justify-between pb-4">
      <h6>
        Profile details
      </h6>
      <lf-button
        v-if="hasPermission(LfPermission.memberEdit)"
        type="secondary"
        size="small"
        @click="edit = true"
      >
        <lf-icon name="pen fa-sharp" />
        Edit attributes
      </lf-button>
    </div>
    <div>
      <!-- Biography -->
      <article v-if="bio?.default" class="border-b border-gray-100 flex py-4">
        <div class="w-5/12">
          <p class="text-small font-semibold mb-1">
            Biography
          </p>
          <lf-contributor-attribute-source :values="bio" />
        </div>
        <div class="w-7/12">
          <lf-contributor-attribute-string
            :data="bio?.default"
          />
        </div>
      </article>

      <!-- Reach -->
      <article v-if="reach?.total > 0" class="border-b border-gray-100 flex py-4">
        <div class="w-5/12">
          <p class="text-small font-semibold mb-1">
            Reach
          </p>
          <lf-contributor-attribute-source
            :values="{
              ...reach,
              total: undefined,
              default: reach.total,
            }"
          />
        </div>
        <div class="w-7/12">
          <p class="text-medium">
            {{ reach?.total }} Followers
          </p>
        </div>
      </article>

      <!-- Custom tags -->
      <article v-for="(values, attr) of attributes" :key="attr" class="border-b border-gray-100 flex py-4">
        <div class="w-5/12">
          <p class="text-small font-semibold mb-1">
            {{ attrInfo[attr]?.label || transformToLabel(attr) }}
          </p>
          <lf-contributor-attribute-source :values="values" />
        </div>
        <div class="w-7/12">
          <lf-contributor-attribute-tags
            v-if="attrInfo[attr]?.type === 'multiSelect'"
            :data="values.default"
          />
          <lf-contributor-attribute-boolean
            v-else-if="attrInfo[attr]?.type === 'boolean'"
            :data="values.default"
          />
          <lf-contributor-attribute-url
            v-else-if="attrInfo[attr]?.type === 'url'"
            :data="`${values.default}`"
          />
          <lf-contributor-attribute-string
            v-else
            :data="`${values.default}`"
          />
        </div>
      </article>

      <!-- Education -->
      <article v-if="education?.default?.length" class="border-b border-gray-100 flex py-4">
        <div class="w-5/12">
          <p class="text-small font-semibold mb-1">
            Education
          </p>
          <lf-contributor-attribute-source
            :values="education"
          />
        </div>
        <div class="w-7/12">
          <div class="-my-4">
            <article v-for="edu of (education.default || [])" :key="edu.major" class="py-4 border-b last:border-b-0 border-gray-100">
              <p class="text-medium mb-0.5">
                {{ edu.major }} {{ edu.specialization }}
              </p>
              <p class="text-small text-gray-500">
                {{ edu.campus }}
              </p>
              <p class="text-small text-gray-500 mt-0.5">
                {{ dateHelper(edu.startDate).isValid() ? dateHelper(edu.startDate).format('MMMM YYYY') : edu.startDate }}
                → {{ dateHelper(edu.endDate).isValid() ? dateHelper(edu.endDate).format('MMMM YYYY') : edu.endDate }}
              </p>
            </article>
          </div>
        </div>
      </article>

      <!-- Certifications -->
      <article v-if="certifications?.default?.length" class="border-b border-gray-100 flex py-4">
        <div class="w-5/12">
          <p class="text-small font-semibold mb-1">
            Certifications
          </p>
          <lf-contributor-attribute-source
            :values="certifications"
          />
        </div>
        <div class="w-7/12">
          <div class="-my-4">
            <article v-for="cert of (certifications.default || [])" :key="cert.title" class="py-4 border-b last:border-b-0 border-gray-100">
              <p class="text-medium mb-0.5">
                {{ cert.title }}
              </p>
              <p class="text-small text-gray-500">
                {{ cert.description }}
              </p>
            </article>
          </div>
        </div>
      </article>

      <!-- Awards -->
      <article v-if="awards?.default?.length" class="border-b border-gray-100 flex py-4">
        <div class="w-5/12">
          <p class="text-small font-semibold mb-1">
            Awards
          </p>
          <lf-contributor-attribute-source
            :values="awards"
          />
        </div>
        <div class="w-7/12">
          <div class="-my-4">
            <article v-for="aw of (awards.default || [])" :key="aw" class="py-4 border-b last:border-b-0 border-gray-100">
              <lf-contributor-attribute-string
                :data="aw"
              />
            </article>
          </div>
        </div>
      </article>
    </div>
    <div
      v-if="!bio?.default
        && reach?.total <= 0
        && !education?.default?.length
        && !certifications?.default?.length
        && !awards?.default?.length
        && Object.keys(attributes).length === 0"
      class="pt-2 flex flex-col items-center w-full"
    >
      <lf-icon name="rectangle-list" :size="80" class="text-gray-300" />
      <p class="text-center pt-3 text-medium text-gray-400">
        No profile details yet
      </p>
    </div>
  </section>
  <app-member-manage-attributes-drawer
    v-if="edit"
    v-model="edit"
    :member="props.contributor"
  />
</template>

<script setup lang="ts">
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import AppMemberManageAttributesDrawer from '@/modules/member/components/member-manage-attributes-drawer.vue';
import { computed, ref } from 'vue';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { useMemberStore } from '@/modules/member/store/pinia';
import { storeToRefs } from 'pinia';
import LfContributorAttributeTags
  from '@/modules/contributor/components/details/attributes/contributor-attribute-tags.vue';
import LfContributorAttributeBoolean
  from '@/modules/contributor/components/details/attributes/contributor-attribute-boolean.vue';
import LfContributorAttributeString
  from '@/modules/contributor/components/details/attributes/contributor-attribute-string.vue';
import LfContributorAttributeSource
  from '@/modules/contributor/components/details/attributes/contributor-attribute-source.vue';
import LfContributorAttributeUrl
  from '@/modules/contributor/components/details/attributes/contributor-attribute-url.vue';
import type { Contributor } from '@/modules/contributor/types/Contributor';
import { dateHelper } from '@/shared/date-helper/date-helper';

const props = defineProps<{
  contributor: Contributor,
}>();

const { hasPermission } = usePermissions();

const edit = ref<boolean>(false);

const memberStore = useMemberStore();
const { customAttributes } = storeToRefs(memberStore);

const bio = computed(() => props.contributor.attributes?.bio);
const reach = computed(() => props.contributor.reach);
const certifications = computed(() => props.contributor.attributes?.certifications);
const education = computed(() => props.contributor.attributes?.education);
const awards = computed(() => props.contributor.attributes?.awards);

const attrInfo = computed<Record<string, any>>(() => customAttributes.value.reduce((info, attr) => ({
  ...info,
  [attr.name]: attr,
}), {}));

const eliminateAttributes = [
  'bio',
  'url',
  'avatarUrl',
  'emails',
  'jobTitle',
  'workExperiences',
  'certifications',
  'education',
  'awards',
];

const attributes = computed(() => {
  const attrs = { ...props.contributor.attributes };
  eliminateAttributes.forEach((attr) => {
    delete attrs[attr];
  });
  return attrs;
});

const transformToLabel = (property: string) => {
  const label = property.replace(/([A-Z])/g, ' $1').toLowerCase();
  return label.charAt(0).toUpperCase() + label.slice(1);
};

</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsAttributes',
};
</script>
