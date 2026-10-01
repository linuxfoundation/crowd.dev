<template>
  <lf-card v-bind="$attrs">
    <div class="px-5 py-4 flex justify-between items-center">
      <h6>Projects</h6>
    </div>
    <div class="pb-3.5">
      <lf-table class="!overflow-visible">
        <thead>
          <tr>
            <lf-table-head class="pl-5">
              Project
            </lf-table-head>
            <lf-table-head>
              Affiliation
              <el-popover placement="top" width="20rem">
                <template #reference>
                  <lf-icon name="circle-question" :size="14" class="text-secondary-200 font-normal" />
                </template>
                <div class="p-1">
                  <p class="text-small font-semibold mb-2 text-black">
                    Affiliation
                  </p>
                  <p class="text-small text-gray-500 break-normal text-left">
                    Organization(s) that an individual represents while contributing to a project.
                    This association indicates that the person's activities were made in the context
                    of their role within the organization, rather than as an independent
                    contributor.
                  </p>
                </div>
              </el-popover>
            </lf-table-head>
            <lf-table-head>
              Role
              <el-popover placement="top" width="20rem">
                <template #reference>
                  <lf-icon name="circle-question" :size="14" class="text-secondary-200 font-normal" />
                </template>
                <div class="p-1">
                  <p class="text-small font-semibold mb-2 text-black">
                    Maintainer
                  </p>
                  <p class="text-small text-gray-500 break-normal mb-5 text-left">
                    Individual responsible for overseeing and managing code repositories by
                    reviewing and merging pull requests, addressing issues, ensuring code quality,
                    and guiding contributors.
                  </p>
                  <p class="text-small font-semibold mb-2 text-black">
                    Contributor
                  </p>
                  <p class="text-small text-gray-500 break-normal text-left">
                    Someone who has contributed to a project by making changes or additions to its
                    code. Contributions require that code was successfully merged into a repository.
                  </p>
                </div>
              </el-popover>
            </lf-table-head>
            <lf-table-head />
          </tr>
        </thead>

        <tbody>
          <tr v-for="project in (projects || []).slice(0, showMore ? (projects || []).length : 3)" :key="project.id">
            <lf-table-cell class="pl-5">
              <p class="text-medium font-semibold py-1.5">
                {{ project.name }}
              </p>
              <p class="text-small text-gray-500 whitespace-nowrap flex items-center gap-1">
                <lf-icon name="monitor-waveform" />{{ project.activityCount }} {{ parseInt(project.activityCount) > 1 ? 'activities' : 'activity' }}
              </p>
            </lf-table-cell>
            <lf-table-cell>
              <div v-if="Object.keys(project.affiliations).length" class="flex items-center gap-1">
                <lf-contributor-details-projects-affiliation :project="project" />
              </div>
              <div v-else-if="hasPermission(LfPermission.memberEdit)">
                <lf-button
                  type="primary-link"
                  size="small"
                  class="!text-primary-300 hover:!text-primary-600"
                  @click="isAffilationEditOpen = true"
                >
                  <lf-icon name="plus" />Add affiliation
                </lf-button>
              </div>
            </lf-table-cell>
            <lf-table-cell>
              <lf-contributor-details-projects-maintainer :maintainer-roles="getMaintainerRoles(project)" />
            </lf-table-cell>
            <lf-table-cell>
              <lf-dropdown placement="bottom-end" width="160px">
                <template #trigger>
                  <lf-button type="secondary-ghost" size="small" icon-only>
                    <lf-icon name="ellipsis-vertical" type="regular" />
                  </lf-button>
                </template>
                <lf-dropdown-item @click="viewActivity(project.id)">
                  <lf-icon name="eye" />View activity
                </lf-dropdown-item>
                <lf-dropdown-item v-if="hasPermission(LfPermission.memberEdit)" @click="isAffilationEditOpen = true">
                  <lf-icon name="pen fa-sharp" />Edit affiliation
                </lf-dropdown-item>
                <lf-dropdown-item
                  @click="
                    setReportDataModal({
                      contributor: props.contributor,
                      type: ReportDataType.PROJECT_AFFILIATION,
                      attribute: project,
                    })
                  "
                >
                  <lf-icon name="message-exclamation fa-sharp" class="!text-red-500" />Report issue
                </lf-dropdown-item>
              </lf-dropdown>
            </lf-table-cell>
          </tr>
        </tbody>
      </lf-table>
    </div>
    <div v-if="projects.length > 3" class="px-5 py-4">
      <lf-button type="primary-link" size="small" @click="showMore = !showMore">
        Show {{ showMore ? 'less' : 'more' }}
      </lf-button>
    </div>
  </lf-card>
  <lf-contributor-edit-affilations
    v-if="isAffilationEditOpen"
    v-model="isAffilationEditOpen"
    :contributor="props.contributor"
  />
</template>

<script setup lang="ts">
import LfCard from '@/ui-kit/card/Card.vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import { computed, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import type { Contributor, ContributorAffiliation } from '@/modules/contributor/types/Contributor';
import LfContributorEditAffilations from '@/modules/contributor/components/edit/affilations/contributor-affilations-edit.vue';
import LfTable from '@/ui-kit/table/Table.vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfTableCell from '@/ui-kit/table/TableCell.vue';
import LfTableHead from '@/ui-kit/table/TableHead.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import LfContributorDetailsProjectsAffiliation
  from '@/modules/contributor/components/details/overview/project/contributor-details-projects-affiliation.vue';
import LfContributorDetailsProjectsMaintainer
  from '@/modules/contributor/components/details/overview/project/contributor-details-projects-maintainer.vue';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import { LfPermission } from '@/shared/modules/permissions/types/Permissions';
import { ReportDataType } from '@/shared/modules/report-issue/constants/report-data-type.enum';
import { useSharedStore } from '@/shared/pinia/shared.store';

const props = defineProps<{
  contributor: Contributor,
}>();

const router = useRouter();
const route = useRoute();

const { hasPermission } = usePermissions();
const { setReportDataModal } = useSharedStore();

const showMore = ref<boolean>(false);
const isAffilationEditOpen = ref<boolean>(false);

const getAffiliations = (projectId: string) => (props.contributor.affiliations || [])
  .filter((affiliation) => affiliation.segmentId === projectId)
  .reduce((obj: Record<string, ContributorAffiliation[]>, aff: ContributorAffiliation) => {
    if (!obj[aff.organizationId]) {
      return {
        ...obj,
        [aff.organizationId]: [aff],
      };
    }
    return {
      ...obj,
      [aff.organizationId]: [...obj[aff.organizationId], aff],
    };
  }, {});

const projects = computed(() => props.contributor.segments.map((p) => ({
  ...p,
  affiliations: getAffiliations(p.id),
})));

const getMaintainerRoles = (project: any) => props.contributor.maintainerRoles.filter((role) => role.segmentId === project.id);

const viewActivity = (projectId: string) => {
  router.replace({
    hash: '#activities',
    query: {
      ...route.query,
      subProjectId: projectId,
    },
  });
};
</script>

<script lang="ts">
export default {
  name: 'LfContributorDetailsProjects',
};
</script>
