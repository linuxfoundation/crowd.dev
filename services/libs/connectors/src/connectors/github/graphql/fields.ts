export const USER_FIELDS = `
  __typename
  login
  name
  avatarUrl
  isHireable
  url
  bio
  company
  location
  email
  websiteUrl
  databaseId
  organizations(first: 5) {
    nodes {
      __typename
      databaseId
      login
      name
      url
      websiteUrl
      description
      avatarUrl
      twitterUsername
      location
    }
  }
`

export const BOT_FIELDS = `
  __typename
  login
  avatarUrl
  url
  databaseId
`

export const ORGANIZATION_FIELDS = `
  __typename
  avatarUrl
  databaseId
  email
  location
  login
  name
  url
  websiteUrl
`
