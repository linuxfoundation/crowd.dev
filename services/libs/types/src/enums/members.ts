export enum MemberAttributeType {
  BOOLEAN = 'boolean',
  NUMBER = 'number',
  EMAIL = 'email',
  STRING = 'string',
  URL = 'url',
  DATE = 'date',
  MULTI_SELECT = 'multiSelect',
  SPECIAL = 'special',
}

export enum MemberAttributeName {
  ORGANISATION = 'organisation',
  SOURCE_ID = 'sourceId',
  IS_HIREABLE = 'isHireable',
  URL = 'url',
  NAME = 'name',
  AVATAR_URL = 'avatarUrl',
  LOCATION = 'location',
  BIO = 'bio',
  COMPANY = 'company',
  WEBSITE_URL = 'websiteUrl',
  SAMPLE = 'sample',
  JOB_TITLE = 'jobTitle',
  IS_TEAM_MEMBER = 'isTeamMember',
  IS_ORGANIZATION = 'isOrganization',
  IS_BOT = 'isBot',
  TIMEZONE = 'timezone',
  KARMA = 'karma',
  SYNC_REMOTE = 'syncRemote',
  LANGUAGES = 'languages',
  PROGRAMMING_LANGUAGES = 'programmingLanguages',
  SENIORITY_LEVEL = 'seniorityLevel',
  EMAILS = 'emails',
  SKILLS = 'skills',
  COUNTRY = 'country',
  GEO = 'geo',
  YEARS_OF_EXPERIENCE = 'yearsOfExperience',
  EDUCATION = 'education',
  SCHOOLS = 'schools',
  AWARDS = 'awards',
  CERTIFICATIONS = 'certifications',
  WORK_EXPERIENCES = 'workExperiences',
  EXPERTISE = 'expertise',
}

export enum MemberAttributeOpensearch {
  LOCATION = 'obj_location',
  AVATAR_URL = 'obj_avatarUrl',
  LANGUAGES = 'string_arr_languages',
  PROGRAMMING_LANGUAGES = 'string_arr_programmingLanguages',
  TIMEZONE = 'string_timezone',
}

export const MemberAttributes = {
  [MemberAttributeName.ORGANISATION]: {
    name: MemberAttributeName.ORGANISATION,
    label: 'Organisation',
  },
  [MemberAttributeName.SOURCE_ID]: {
    name: MemberAttributeName.SOURCE_ID,
    label: 'Source Id',
  },
  [MemberAttributeName.IS_HIREABLE]: {
    name: MemberAttributeName.IS_HIREABLE,
    label: 'is Hireable',
  },
  [MemberAttributeName.URL]: {
    name: MemberAttributeName.URL,
    label: 'Url',
  },
  [MemberAttributeName.NAME]: {
    name: MemberAttributeName.NAME,
    label: 'Name',
  },
  [MemberAttributeName.AVATAR_URL]: {
    name: MemberAttributeName.AVATAR_URL,
    label: 'Avatar url',
  },
  [MemberAttributeName.LOCATION]: {
    name: MemberAttributeName.LOCATION,
    label: 'Location',
  },
  [MemberAttributeName.BIO]: {
    name: MemberAttributeName.BIO,
    label: 'Bio',
  },
  [MemberAttributeName.WEBSITE_URL]: {
    name: MemberAttributeName.WEBSITE_URL,
    label: 'Website',
  },
  [MemberAttributeName.SAMPLE]: {
    name: MemberAttributeName.SAMPLE,
    label: 'Sample',
  },
  [MemberAttributeName.JOB_TITLE]: {
    name: MemberAttributeName.JOB_TITLE,
    label: 'Job Title',
  },
  [MemberAttributeName.TIMEZONE]: {
    name: MemberAttributeName.TIMEZONE,
    label: 'Timezone',
  },
  [MemberAttributeName.IS_TEAM_MEMBER]: {
    name: MemberAttributeName.IS_TEAM_MEMBER,
    label: 'is Team Member',
  },
  [MemberAttributeName.IS_ORGANIZATION]: {
    name: MemberAttributeName.IS_ORGANIZATION,
    label: 'is Organization',
  },
  [MemberAttributeName.IS_BOT]: {
    name: MemberAttributeName.IS_BOT,
    label: 'is Bot',
  },
  [MemberAttributeName.KARMA]: {
    name: MemberAttributeName.KARMA,
    label: 'Karma',
  },
  [MemberAttributeName.SYNC_REMOTE]: {
    name: MemberAttributeName.SYNC_REMOTE,
    label: 'Sync remote',
  },
  [MemberAttributeName.COMPANY]: {
    name: MemberAttributeName.COMPANY,
    label: 'Company',
  },
  [MemberAttributeName.SENIORITY_LEVEL]: {
    name: MemberAttributeName.SENIORITY_LEVEL,
    label: 'Seniority Level',
  },
  [MemberAttributeName.EMAILS]: {
    name: MemberAttributeName.EMAILS,
    label: 'Emails',
  },
  [MemberAttributeName.SKILLS]: {
    name: MemberAttributeName.SKILLS,
    label: 'Skills',
  },
  [MemberAttributeName.COUNTRY]: {
    name: MemberAttributeName.COUNTRY,
    label: 'Country',
  },
  [MemberAttributeName.PROGRAMMING_LANGUAGES]: {
    name: MemberAttributeName.PROGRAMMING_LANGUAGES,
    label: 'Programming Languages',
  },
  [MemberAttributeName.LANGUAGES]: {
    name: MemberAttributeName.LANGUAGES,
    label: 'Languages',
  },
  [MemberAttributeName.YEARS_OF_EXPERIENCE]: {
    name: MemberAttributeName.YEARS_OF_EXPERIENCE,
    label: 'Years of Experience',
  },
  [MemberAttributeName.EDUCATION]: {
    name: MemberAttributeName.EDUCATION,
    label: 'Education',
  },
  [MemberAttributeName.SCHOOLS]: {
    name: MemberAttributeName.SCHOOLS,
    label: 'Schools',
  },
  [MemberAttributeName.AWARDS]: {
    name: MemberAttributeName.AWARDS,
    label: 'Awards',
  },
  [MemberAttributeName.CERTIFICATIONS]: {
    name: MemberAttributeName.CERTIFICATIONS,
    label: 'Certifications',
  },
  [MemberAttributeName.WORK_EXPERIENCES]: {
    name: MemberAttributeName.WORK_EXPERIENCES,
    label: 'Work Experiences',
  },
  [MemberAttributeName.EXPERTISE]: {
    name: MemberAttributeName.EXPERTISE,
    label: 'Expertise',
  },
}

export enum MemberIdentityType {
  USERNAME = 'username',
  EMAIL = 'email',
}

export enum MemberBotDetection {
  CONFIRMED_BOT = 'confirmed_bot',
  SUSPECTED_BOT = 'suspected_bot',
  NOT_BOT = 'not_bot',
}
