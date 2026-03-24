/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  DiscussionNoteSchema,
  ExpandedCommitSchema,
  IssueNoteSchema,
  IssueSchema,
  MergeRequestNoteSchema,
  MergeRequestSchema,
  ProjectSchema,
  ProjectStarrerSchema,
  UserSchema,
} from '@gitbeaker/rest'

export enum GitlabActivityType {
  MERGE_REQUEST_OPENED = 'merge_request-opened',
  MERGE_REQUEST_CLOSED = 'merge_request-closed',
  MERGE_REQUEST_REVIEW_REQUESTED = 'merge_request-review-requested',
  MERGE_REQUEST_REVIEW_APPROVED = 'merge_request-review-approved',
  MERGE_REQUEST_REVIEW_CHANGES_REQUESTED = 'merge_request-review-changes-requested',
  MERGE_REQUEST_ASSIGNED = 'merge_request-assigned',
  MERGE_REQUEST_MERGED = 'merge_request-merged',
  MERGE_REQUEST_COMMENT = 'merge_request-comment',
  ISSUE_OPENED = 'issues-opened',
  ISSUE_CLOSED = 'issues-closed',
  FORK = 'fork',
  STAR = 'star',
  ISSUE_COMMENT = 'issue-comment',
  AUTHORED_COMMIT = 'authored-commit',
}

export enum GitlabWebhookType {
  ISSUE_OPENED_OR_UPDATED = 'issue-opened-or-updated',
  ISSUE_CLOSED = 'issue-closed',
  ISSUE_COMMENT = 'issue_comment',
  MERGE_REQUEST_OPENED = 'merge_request-opened',
  MERGE_REQUEST_CLOSED = 'merge_request-closed',
  MERGE_REQUEST_COMMENT = 'merge_request-comment',
  MERGE_REQUEST_MERGED = 'merge_request-merged',
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface GitlabActivityData<T = any> {
  data: T
  user?: UserSchema
  relatedUser?: UserSchema
  relatedData?: Record<string, unknown>
}

export type GitlabForkData = GitlabActivityData<ProjectSchema>
export type GitlabStarData = GitlabActivityData<ProjectStarrerSchema>
export type GitlabIssueData = GitlabActivityData<IssueSchema>
export type GitlabIssueCommentData = GitlabActivityData<IssueNoteSchema>
export type GitlabDisccusionCommentData = GitlabActivityData<DiscussionNoteSchema>
export type GitlabMergeRequestData = GitlabActivityData<MergeRequestSchema>
export type GitlabMergeRequestCommentData = GitlabActivityData<MergeRequestNoteSchema>
export type GitlabMergeRequestCommitData = GitlabActivityData<ExpandedCommitSchema>

export interface GitlabApiResult<T> {
  data: T
  nextPage: number | null
}

export interface GitlabApiData<T> {
  type: GitlabActivityType | GitlabWebhookType
  data: GitlabActivityData<T>
  projectId: string
  pathWithNamespace: string
  isWebhook?: boolean
}

export enum GitlabStreamType {
  ROOT = 'root',
  ISSUES = 'issues',
  ISSUE_DISCUSSIONS = 'issue_discussions',
  MERGE_REQUESTS = 'merge_requests',
  MERGE_REQUEST_DISCUSSIONS_AND_EVENTS = 'merge_request_discussions_and_events',
  MERGE_REQUEST_COMMITS = 'merge_request_commits',
  COMMITS = 'commits',
  STARS = 'stars',
  FORKS = 'forks',
}

export interface GitlabRootStream {
  projects: {
    id: string
    path_with_namespace: string
  }[]
}

export interface GitlabBasicStream {
  projectId: string
  pathWithNamespace: string
  page: number
  meta?: Record<string, unknown>
}
interface GitlabWebhookBase {
  object_kind: string
  event_type: string
  user: {
    id: number
    name: string
    username: string
    avatar_url: string
    email: string
  }
  project: {
    id: number
    name: string
    description: string
    web_url: string
    avatar_url: string | null
    git_ssh_url: string
    git_http_url: string
    namespace: string
    visibility_level: number
    path_with_namespace: string
    default_branch: string
    ci_config_path: string
    homepage: string
    url: string
    ssh_url: string
    http_url: string
  }
  repository: {
    name: string
    url: string
    description: string
    homepage: string
  }
}

export interface GitlabIssueWebhook extends GitlabWebhookBase {
  object_kind: 'issue'
  object_attributes: {
    author_id: number
    closed_at: string | null
    confidential: boolean
    created_at: string
    description: string
    discussion_locked: boolean | null
    due_date: string | null
    id: number
    iid: number
    last_edited_at: string | null
    last_edited_by_id: number | null
    milestone_id: number | null
    moved_to_id: number | null
    duplicated_to_id: number | null
    project_id: number
    relative_position: number | null
    state_id: number
    time_estimate: number
    title: string
    updated_at: string
    updated_by_id: number | null
    weight: number | null
    health_status: string | null
    type: string
    url: string
    total_time_spent: number
    time_change: number
    human_total_time_spent: string | null
    human_time_change: string | null
    human_time_estimate: string | null
    assignee_ids: number[]
    assignee_id: number | null
    labels: string[]
    state: string
    severity: string
    customer_relations_contacts: any[]
    action: string
  }
  labels: string[]
  changes: {
    [key: string]: {
      previous: any
      current: any
    }
  }
}

export interface GitlabMergeRequestWebhook extends GitlabWebhookBase {
  object_kind: 'merge_request'
  object_attributes: {
    assignee_id: number | null
    author_id: number
    created_at: string
    description: string
    draft: boolean
    head_pipeline_id: number | null
    id: number
    iid: number
    last_edited_at: string | null
    last_edited_by_id: number | null
    merge_commit_sha: string | null
    merge_error: string | null
    merge_params: {
      force_remove_source_branch: string
    }
    merge_status: string
    merge_user_id: number | null
    merge_when_pipeline_succeeds: boolean
    milestone_id: number | null
    source_branch: string
    source_project_id: number
    state_id: number
    target_branch: string
    target_project_id: number
    time_estimate: number
    title: string
    updated_at: string
    updated_by_id: number | null
    prepared_at: string
    assignee_ids: number[]
    blocking_discussions_resolved: boolean
    detailed_merge_status: string
    first_contribution: boolean
    human_time_change: string | null
    human_time_estimate: string | null
    human_total_time_spent: string | null
    labels: string[]
    last_commit: {
      id: string
      message: string
      title: string
      timestamp: string
      url: string
      author: {
        name: string
        email: string
      }
    }
    reviewer_ids: number[]
    source: {
      id: number
      name: string
      description: string
      web_url: string
      avatar_url: string | null
      git_ssh_url: string
      git_http_url: string
      namespace: string
      visibility_level: number
      path_with_namespace: string
      default_branch: string
      ci_config_path: string
      homepage: string
      url: string
      ssh_url: string
      http_url: string
    }
    state: string
    target: {
      id: number
      name: string
      description: string
      web_url: string
      avatar_url: string | null
      git_ssh_url: string
      git_http_url: string
      namespace: string
      visibility_level: number
      path_with_namespace: string
      default_branch: string
      ci_config_path: string
      homepage: string
      url: string
      ssh_url: string
      http_url: string
    }
    time_change: number
    total_time_spent: number
    url: string
    work_in_progress: boolean
    approval_rules: any[]
    action: string
  }
  labels: string[]
  changes: {
    [key: string]: {
      previous: any
      current: any
    }
  }
}

export interface GitlabMergeRequestCommentWebhook extends GitlabWebhookBase {
  object_kind: 'note'
  object_attributes: {
    attachment: null
    author_id: number
    change_position: null
    commit_id: null
    created_at: string
    discussion_id: string
    id: number
    line_code: null
    note: string
    noteable_id: number
    noteable_type: string
    original_position: null
    position: null
    project_id: number
    resolved_at: null
    resolved_by_id: null
    resolved_by_push: null
    st_diff: null
    system: boolean
    type: null
    updated_at: string
    updated_by_id: null
    description: string
    url: string
    action: string
  }
  merge_request: {
    assignee_id: number
    author_id: number
    created_at: string
    description: string
    draft: boolean
    head_pipeline_id: number | null
    id: number
    iid: number
    last_edited_at: string | null
    last_edited_by_id: number | null
    merge_commit_sha: string | null
    merge_error: string | null
    merge_params: {
      force_remove_source_branch: string
    }
    merge_status: string
    merge_user_id: number | null
    merge_when_pipeline_succeeds: boolean
    milestone_id: number | null
    source_branch: string
    source_project_id: number
    state_id: number
    target_branch: string
    target_project_id: number
    time_estimate: number
    title: string
    updated_at: string
    updated_by_id: number
    prepared_at: string
    assignee_ids: number[]
    blocking_discussions_resolved: boolean
    detailed_merge_status: string
    first_contribution: boolean
    human_time_change: string | null
    human_time_estimate: string | null
    human_total_time_spent: string | null
    labels: string[]
    last_commit: {
      id: string
      message: string
      title: string
      timestamp: string
      url: string
      author: {
        name: string
        email: string
      }
    }
    reviewer_ids: number[]
    source: {
      id: number
      name: string
      description: string
      web_url: string
      avatar_url: string | null
      git_ssh_url: string
      git_http_url: string
      namespace: string
      visibility_level: number
      path_with_namespace: string
      default_branch: string
      ci_config_path: string
      homepage: string
      url: string
      ssh_url: string
      http_url: string
    }
    state: string
    target: {
      id: number
      name: string
      description: string
      web_url: string
      avatar_url: string | null
      git_ssh_url: string
      git_http_url: string
      namespace: string
      visibility_level: number
      path_with_namespace: string
      default_branch: string
      ci_config_path: string
      homepage: string
      url: string
      ssh_url: string
      http_url: string
    }
    time_change: number
    total_time_spent: number
    url: string
    work_in_progress: boolean
    approval_rules: any[]
  }
}

export interface GitlabIssueCommentWebhook extends GitlabWebhookBase {
  object_kind: 'note'
  object_attributes: {
    attachment: null
    author_id: number
    change_position: null
    commit_id: null
    created_at: string
    discussion_id: string
    id: number
    line_code: null
    note: string
    noteable_id: number
    noteable_type: string
    original_position: null
    position: null
    project_id: number
    resolved_at: null
    resolved_by_id: null
    resolved_by_push: null
    st_diff: null
    system: boolean
    type: null
    updated_at: string
    updated_by_id: null
    description: string
    url: string
    action: string
  }
  issue: {
    author_id: number
    closed_at: string | null
    confidential: boolean
    created_at: string
    description: string
    discussion_locked: boolean | null
    due_date: string | null
    id: number
    iid: number
    last_edited_at: string | null
    last_edited_by_id: number | null
    milestone_id: number | null
    moved_to_id: number | null
    duplicated_to_id: number | null
    project_id: number
    relative_position: number
    state_id: number
    time_estimate: number
    title: string
    updated_at: string
    updated_by_id: number | null
    weight: number | null
    health_status: string | null
    type: string
    url: string
    total_time_spent: number
    time_change: number
    human_total_time_spent: string | null
    human_time_change: string | null
    human_time_estimate: string | null
    assignee_ids: number[]
    assignee_id: number | null
    labels: string[]
    state: string
    severity: string
    customer_relations_contacts: any[]
  }
}

export interface GitlabWebhook {
  data:
    | GitlabIssueWebhook
    | GitlabIssueCommentWebhook
    | GitlabMergeRequestWebhook
    | GitlabMergeRequestCommentWebhook
  date: string
  headers: Record<string, string>
}

export { IGitlabIntegrationSettings as GitLabSettings } from '@crowd/types'

export interface GitlabPlatformSettings {
  clientId: string
  clientSecret: string
  callbackUrl: string
  webhookToken: string
}
