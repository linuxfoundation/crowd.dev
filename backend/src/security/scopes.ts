export const SCOPES = {
  READ_MEMBERS: 'read:members',
  READ_ORGANIZATIONS: 'read:organizations',
  WRITE_ORGANIZATIONS: 'write:organizations',
  READ_MEMBER_IDENTITIES: 'read:member-identities',
  WRITE_MEMBER_IDENTITIES: 'write:member-identities',
  READ_MAINTAINER_ROLES: 'read:maintainer-roles',
  READ_WORK_EXPERIENCES: 'read:work-experiences',
  WRITE_WORK_EXPERIENCES: 'write:work-experiences',
  READ_PROJECT_AFFILIATIONS: 'read:project-affiliations',
  WRITE_PROJECT_AFFILIATIONS: 'write:project-affiliations',
  READ_AFFILIATIONS: 'read:affiliations',
} as const

export type Scope = (typeof SCOPES)[keyof typeof SCOPES]
