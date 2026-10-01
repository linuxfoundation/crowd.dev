<template>
  <lf-modal
    v-if="computedVisible"
    v-model="computedVisible"
    header-title="Edit tags"
    :pre-title="member?.displayName ?? ''"
    width="42rem"
    content-class="!overflow-unset"
  >
    <div class="px-6 pb-6">
      <form v-if="modelValue" class="tags-form">
        <app-tag-autocomplete-input
          v-model="editTagsModel"
          :fetch-fn="fields.tags.fetchFn"
          :mapper-fn="fields.tags.mapperFn"
          create-if-not-found
          placeholder="Type to search/create tags"
        />
      </form>
    </div>

    <div
      class="bg-gray-50 rounded-b-md flex items-center justify-end py-4 px-6"
    >
      <lf-button
        type="bordered"
        size="medium"
        class="mr-3"
        @click="handleCancel"
      >
        Cancel
      </lf-button>
      <lf-button
        type="primary"
        size="medium"
        :disabled="isSubmitDisabled"
        @click="handleSubmit"
      >
        Submit
      </lf-button>
    </div>
  </lf-modal>
</template>

<script>
import { MemberModel } from '@/modules/member/member-model';
import AppTagAutocompleteInput from '@/modules/tag/components/tag-autocomplete-input.vue';
import { FormSchema } from '@/shared/form/form-schema';
import { mapActions } from 'vuex';
import { storeToRefs } from 'pinia';
import { useMemberStore } from '@/modules/member/store/pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { getSegmentsFromProjectGroup } from '@/utils/segments';
import usePermissions from '@/shared/modules/permissions/helpers/usePermissions';
import useProductTracking from '@/shared/modules/monitoring/useProductTracking';
import {
  EventType,
  FeatureEventKey,
} from '@/shared/modules/monitoring/types/event';
import LfButton from '@/ui-kit/button/Button.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';

const { trackEvent } = useProductTracking();

const memberStore = useMemberStore();
const { selectedMembers } = storeToRefs(memberStore);

const { fields } = MemberModel;
const formSchema = new FormSchema([fields.tags]);

export default {
  name: 'AppTagPopover',
  components: { AppTagAutocompleteInput, LfModal, LfButton },

  props: {
    modelValue: {
      type: Boolean,
      default: () => false,
    },
    member: {
      type: Object,
      default: () => null,
    },
  },
  emits: ['reload', 'update:modelValue'],

  setup() {
    const { hasAccessToSegmentId } = usePermissions();
    return { hasAccessToSegmentId };
  },

  data() {
    return {
      loading: false,
      editTagsModel: [],
      editTagsInCommon: [],
    };
  },

  computed: {
    fields() {
      return fields;
    },
    computedVisible: {
      get() {
        return this.modelValue;
      },
      set() {
        this.$emit('update:modelValue', false);
      },
    },
    membersToUpdate() {
      return this.member ? [this.member] : selectedMembers.value;
    },
    selectedProjectGroup() {
      const lsSegmentsStore = useLfSegmentsStore();
      const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

      return selectedProjectGroup.value;
    },
    isSubmitDisabled() {
      const segments = this.member
        ? (this.member.segmentIds
          ?? this.member.segments?.map((s) => s.id)
          ?? getSegmentsFromProjectGroup(this.selectedProjectGroup))
        : getSegmentsFromProjectGroup(this.selectedProjectGroup);

      return !segments.some((s) => this.hasAccessToSegmentId(s));
    },
  },

  watch: {
    modelValue: {
      async handler(newValue) {
        if (newValue) {
          await this.prepareUpdateTags();
        }
      },
    },
  },

  methods: {
    ...mapActions({
      doBulkUpdateMembersTags: 'member/doBulkUpdateMembersTags',
    }),

    prepareUpdateTags() {
      this.editTagsModel = this.membersToUpdate.reduce((acc, item, index) => {
        let { tags } = formSchema.initialValues({
          tags: item.tags,
        });
        if (index > 0) {
          tags = tags.filter(
            (tag) => acc.filter((t) => t.id === tag.id).length > 0,
          );
        }
        return tags;
      }, []);
      this.editTagsInCommon = [...this.editTagsModel];
    },

    async handleSubmit() {
      this.loading = true;

      const segments = this.member
        ? (this.member.segmentIds
          ?? this.member.segments?.map((s) => s.id)
          ?? getSegmentsFromProjectGroup(this.selectedProjectGroup))
        : getSegmentsFromProjectGroup(this.selectedProjectGroup);

      trackEvent({
        key: FeatureEventKey.EDIT_MEMBER_TAGS,
        type: EventType.FEATURE,
      });

      await this.doBulkUpdateMembersTags({
        members: [...this.membersToUpdate],
        tagsInCommon: this.editTagsInCommon,
        tagsToSave: this.editTagsModel,
        segments,
      });

      this.loading = false;
      this.computedVisible = false;
      this.$emit('reload', true);
      return null;
    },

    handleCancel() {
      this.editTagsModel = [];
      this.editTagsInCommon = [];
      this.computedVisible = false;
    },
  },
};
</script>
