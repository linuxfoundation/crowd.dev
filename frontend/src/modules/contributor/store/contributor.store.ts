// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { defineStore, storeToRefs } from 'pinia';
import { ref } from 'vue';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { ContributorApiService } from '@/modules/contributor/services/contributor.api.service';
import type {
  Contributor,
  ContributorAffiliation,
  ContributorAttribute,
  ContributorIdentity,
  CreateContributorIdentity,
  UpdateContributorIdentityPayload,
} from '@/modules/contributor/types/Contributor';
import { ContributorIdentitiesApiService } from '@/modules/contributor/services/contributor.identities.api.service';
import { MergeActionsService } from '@/shared/modules/merge/services/merge-actions.service';
import type { MergeAction } from '@/shared/modules/merge/types/MemberActions';
import type { MemberOrganization, Organization } from '@/modules/organization/types/Organization';
import { ContributorOrganizationsApiService } from '@/modules/contributor/services/contributor.organizations.api.service';
import { ContributorAffiliationsApiService } from '@/modules/contributor/services/contributor.affiliations.api.service';
import { ContributorAttributesApiService } from '@/modules/contributor/services/contributor.attributes.api.service';

const getPureContributor = (contributor: Contributor) =>
  Object.fromEntries(
    Object.entries(contributor).filter(
      ([key]) => !['attributes', 'affiliations', 'organizations', 'identities'].includes(key),
    ),
  );

export const useContributorStore = defineStore('contributor', () => {
  // shortcut: `as Contributor` casts keep the spread of a possibly null contributor. revisit: non-null default
  const contributor = ref<Contributor | null>(null);

  function getContributorMergeActions(id: string): Promise<MergeAction[]> {
    return MergeActionsService.list(id).then((mergeActions) => {
      contributor.value = {
        ...contributor.value,
        activitySycning: mergeActions.length > 0 ? mergeActions[0] : null,
      } as Contributor;
      return Promise.resolve(mergeActions);
    });
  }

  function updateContributor(id: string, data: Partial<Contributor>) {
    return ContributorApiService.update(id, data).then((updated) => {
      contributor.value = {
        ...contributor.value,
        ...updated,
      };
      return Promise.resolve(updated);
    });
  }

  function setIdentities(identities: ContributorIdentity[]) {
    contributor.value = {
      ...contributor.value,
      identities,
    } as Contributor;
    return Promise.resolve(identities);
  }

  function getContributorIdentities(id: string) {
    const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
    return ContributorIdentitiesApiService.list(id, [
      selectedProjectGroup.value?.id as string,
    ]).then(setIdentities);
  }

  function createContributorIdentities(
    memberId: string,
    identities: CreateContributorIdentity[],
  ): Promise<ContributorIdentity[]> {
    return ContributorIdentitiesApiService.createMultiple(memberId, identities).then(setIdentities);
  }

  function updateContributorIdentity(
    memberId: string,
    id: string,
    identity: UpdateContributorIdentityPayload,
  ): Promise<ContributorIdentity[]> {
    return ContributorIdentitiesApiService.update(memberId, id, identity).then(setIdentities);
  }

  function deleteContributorIdentity(memberId: string, id: string): Promise<ContributorIdentity[]> {
    return ContributorIdentitiesApiService.delete(memberId, id).then(setIdentities);
  }

  function setOrganizations(organizations: Organization[]) {
    contributor.value = {
      ...contributor.value,
      organizations,
    } as Contributor;
    return Promise.resolve(organizations);
  }

  function getContributorOrganizations(memberId: string) {
    const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
    return ContributorOrganizationsApiService.list(memberId, [
      selectedProjectGroup.value?.id as string,
    ]).then(setOrganizations);
  }

  function createContributorOrganization(
    memberId: string,
    organization: Partial<MemberOrganization>,
  ): Promise<Organization[]> {
    return ContributorOrganizationsApiService.create(memberId, organization).then(setOrganizations);
  }

  function updateContributorOrganization(
    memberId: string,
    id: string,
    organization: Partial<MemberOrganization>,
  ): Promise<Organization[]> {
    return ContributorOrganizationsApiService.update(memberId, id, organization).then(
      setOrganizations,
    );
  }

  function deleteContributorOrganization(memberId: string, id: string): Promise<Organization[]> {
    return ContributorOrganizationsApiService.delete(memberId, id).then(setOrganizations);
  }

  function setAffiliations(affiliations: ContributorAffiliation[]) {
    contributor.value = {
      ...contributor.value,
      affiliations,
    } as Contributor;
    return Promise.resolve(affiliations);
  }

  function getContributorAffiliations(memberId: string) {
    const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
    return ContributorAffiliationsApiService.list(memberId, [
      selectedProjectGroup.value?.id as string,
    ]).then(setAffiliations);
  }

  function updateContributorAffiliations(
    memberId: string,
    affiliations: Partial<ContributorAffiliation>[],
  ): Promise<ContributorAffiliation[]> {
    return ContributorAffiliationsApiService.updateMultiple(memberId, affiliations).then(
      setAffiliations,
    );
  }

  function setAttributes(attributes: Record<string, ContributorAttribute>) {
    contributor.value = {
      ...contributor.value,
      attributes,
    } as Contributor;
    return Promise.resolve(attributes);
  }

  function getContributorAttributes(memberId: string) {
    const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
    return ContributorAttributesApiService.list(memberId, [
      selectedProjectGroup.value?.id as string,
    ]).then(setAttributes);
  }

  function updateContributorAttributes(
    memberId: string,
    attributes: Record<string, ContributorAttribute>,
  ): Promise<Record<string, ContributorAttribute>> {
    return ContributorAttributesApiService.update(memberId, attributes).then(setAttributes);
  }

  function getContributor(id: string): Promise<Contributor> {
    const { selectedProjectGroup } = storeToRefs(useLfSegmentsStore());
    const ignoreError = () => undefined;
    getContributorIdentities(id).catch(ignoreError);
    getContributorOrganizations(id).catch(ignoreError);
    getContributorAttributes(id).catch(ignoreError);
    getContributorAffiliations(id).catch(ignoreError);

    return ContributorApiService.find(id, [selectedProjectGroup.value?.id as string]).then(
      (found) => {
        contributor.value = {
          ...contributor.value,
          ...getPureContributor(found),
        } as Contributor;
        getContributorMergeActions(id).catch(ignoreError);
        return Promise.resolve(contributor.value);
      },
    );
  }

  return {
    contributor,
    getContributor,
    getContributorMergeActions,
    updateContributor,
    setIdentities,
    getContributorIdentities,
    createContributorIdentities,
    updateContributorIdentity,
    deleteContributorIdentity,
    setOrganizations,
    getContributorOrganizations,
    createContributorOrganization,
    updateContributorOrganization,
    deleteContributorOrganization,
    setAffiliations,
    getContributorAffiliations,
    updateContributorAffiliations,
    setAttributes,
    getContributorAttributes,
    updateContributorAttributes,
  };
});
