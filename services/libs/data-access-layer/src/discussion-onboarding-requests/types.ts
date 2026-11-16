export interface IDbDiscussionOnboardingRequest {
  sourceUrl: string
  discussionId: string
  commentId: string
  notes: string[]
  doneNotifiedAt: string | null
  createdAt: string
  updatedAt: string
}

export type IDbDiscussionOnboardingRequestCreate = Pick<
  IDbDiscussionOnboardingRequest,
  'sourceUrl' | 'discussionId' | 'commentId'
> &
  Partial<Pick<IDbDiscussionOnboardingRequest, 'notes'>>
