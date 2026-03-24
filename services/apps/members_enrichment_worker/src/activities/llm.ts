import _ from 'lodash'

import { LlmService } from '@crowd/common_services'
import { dbStoreQx } from '@crowd/data-access-layer'
import { IMemberOriginalData } from '@crowd/types'

import { svc } from '../service'
import {
  IMemberEnrichmentDataNormalized,
  IMemberEnrichmentDataNormalizedOrganization,
} from '../types'

/**
 * Among given profiles, find the one that belongs to the same person as the given member profile.
 * @param memberProfile
 * @param linkedinProfiles
 */
export async function findRelatedLinkedinProfilesWithLLM(
  memberId: string,
  memberProfile: IMemberOriginalData,
  linkedinProfiles: IMemberEnrichmentDataNormalized[],
): Promise<{ profileIndex: number }> {
  // Some organizations have too many identities, which exceeds the llm input token limit,
  // so we deduplicate the identities, prioritize verified ones, and select the top 50 per organization.
  memberProfile.organizations = memberProfile.organizations?.map((org) => ({
    ...org,
    identities: _.uniqBy(org.identities, (i) => `${i.platform}:${i.value}`)
      .sort((a, b) => (a.verified === b.verified ? 0 : a.verified ? -1 : 1))
      .slice(0, 50),
  }))

  const prompt = `
  "You are an expert at analyzing and matching personal profiles. I will provide you with the details of a member profile and an array of LinkedIn profiles in JSON format. Your task is to analyze the data and return only the index of the profile that most likely belongs to the member.
  
  Instructions:
      Match the profiles based on flexible criteria, allowing partial matches or similarities.
      Output valid JSON only. The JSON should include the matched profile.
      The JSON should include the matched profile's index from the input array, 0-indexed. If no match is found, return "profileIndex": null.
  Considerations for Matching:
    Name Similarity: Consider at most 2 edit distances, use character tokenization.
    Job Titles and Companies: Look for overlaps in current or past job titles and companies.
    Location: Prioritize profiles with overlapping or similar locations.
    Education and Skills: Check for shared elements in education and skillsets.
    If there are contradictory data, don't return the profile
    If a profile matches at least two strong criterions (e.g., name, job, or location) and has no contradictory information, it is a plausible match.
  
    ### Member Profile:
      ${JSON.stringify(memberProfile)}
  
    ### LinkedIn Profiles:
      ${JSON.stringify(linkedinProfiles)}
  
    
    Expected Output: 
    If a match is found: 
    { 
      "profileIndex": 0, /* 0-indexed index of the matched profile */ 
    }
  
    If no match is found: 
    { 
      "profileIndex": null
    }
  
    Return exactly one profile in valid JSON format.
      If no match is found, return null in profileIndex.
      Ensure the response is a **valid and complete JSON**.
      DO NOT output anything else.
      Output ONLY valid JSON
    `

  const llmService = new LlmService(
    dbStoreQx(svc.postgres.writer),
    {
      accessKeyId: process.env['CROWD_AWS_BEDROCK_ACCESS_KEY_ID'],
      secretAccessKey: process.env['CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY'],
    },
    svc.log,
  )

  const result = await llmService.findRelatedLinkedinProfiles(memberId, prompt)
  return result.result
}

/**
 * Squash multiple-value attributes into a single value based on the best criteria.
 * @param memberProfile
 * @param linkedinProfiles
 */
export async function squashMultipleValueAttributesWithLLM(
  memberId: string,
  attributes: {
    [key: string]: unknown[]
  },
): Promise<{ [key: string]: unknown }> {
  const prompt = `
      I have an object with attributes structured as follows:
      
      <json> ${JSON.stringify(attributes)} </json>
  
      The possible attributes include:
  
      # avatarUrl (string): Represents URLs for user profile pictures.
      # jobTitle (string): Represents job titles or roles.
      # bio (string): Represents user biographies or descriptions.
      # location (string): Represents user locations.
      
      Each attribute has an array of possible values, and the task is to determine the best value for each attribute based on the following criteria:
  
      General rules:
        - Select the most relevant and accurate value for each attribute.
        - Repeated information across values can be considered a strong indicator.
  
      Specific rules:
        For avatarUrl:
          - Prefer the URL pointing to the highest-quality, professional, or clear image.
          - Exclude any broken or invalid URLs.
        For jobTitle:
          - Choose the most precise, specific, and professional title (e.g., "Software Engineer" over "Engineer").
          - If job titles indicate a hierarchy, select the one representing the highest level (e.g., "Senior Software Engineer" over "Software Engineer").
        For bio:
          - Select the most detailed, relevant, and grammatically accurate description.
          - Avoid overly generic or vague descriptions.
        For location:
          - Prioritize values that are specific and precise (e.g., "Berlin, Germany" over just "Germany").
          - Ensure the location format is complete and includes necessary details (e.g., city and country).
  
      Use the provided attributes and their values to make the best possible selection for each attribute, ensuring the choices align with professional, specific, and practical standards.
  
      OUTPUT FORMAT:
        - Return a single JSON object with exactly the following fields: avatarUrl, jobTitle, bio, location.
        - You must return ONLY valid JSON.
        - Do NOT include explanations, code fences, or any extra text.
        - The JSON must be valid, start with '{' and end with '}'.
  
      JSON SCHEMA:
      {
        "avatarUrl": "<selected URL or empty string if none valid>",
        "jobTitle": "<selected job title or empty string if none valid>",
        "bio": "<selected bio or empty string if none valid>",
        "location": "<selected location or empty string if none valid>"
      }
    `

  const llmService = new LlmService(
    dbStoreQx(svc.postgres.writer),
    {
      accessKeyId: process.env['CROWD_AWS_BEDROCK_ACCESS_KEY_ID'],
      secretAccessKey: process.env['CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY'],
    },
    svc.log,
  )

  const result = await llmService.squashMultipleValueAttributes<{ [key: string]: unknown }>(
    memberId,
    prompt,
  )
  return result.result
}

export async function findWhichLinkedinProfileToUseAmongScraperResult(
  memberId: string,
  memberData: IMemberOriginalData,
  profiles: IMemberEnrichmentDataNormalized[],
): Promise<{
  selected: IMemberEnrichmentDataNormalized
  discarded: IMemberEnrichmentDataNormalized[]
}> {
  const categorized = {
    selected: null,
    discarded: [],
  }
  const profilesFromVerifiedIdentities: IMemberEnrichmentDataNormalized[] = []
  const profilesFromUnverfiedIdentities: IMemberEnrichmentDataNormalized[] = []

  // ignore linkedin scraper sources, when there's only one source returning linkedin handle
  for (const profile of profiles) {
    if (profile.metadata.isFromVerifiedSource) {
      profilesFromVerifiedIdentities.push(profile)
    } else {
      profilesFromUnverfiedIdentities.push(profile)
    }
  }

  if (profilesFromVerifiedIdentities.length > 0) {
    if (profilesFromVerifiedIdentities.length > 1) {
      // ASK LLM
      const result = await findRelatedLinkedinProfilesWithLLM(
        memberId,
        memberData,
        profilesFromVerifiedIdentities,
      )

      // check if empty object
      if (result.profileIndex !== null) {
        categorized.selected = profilesFromVerifiedIdentities[result.profileIndex]
        // add profiles not selected to discarded
        for (let i = 0; i < profilesFromVerifiedIdentities.length; i++) {
          if (i !== result.profileIndex) {
            categorized.discarded.push(profilesFromVerifiedIdentities[i])
          }
        }
      } else {
        // if no match found, we should discard all profiles from verified identities
        categorized.discarded = profilesFromVerifiedIdentities
      }
    } else {
      categorized.selected = profilesFromVerifiedIdentities[0]
    }
  }

  if (profilesFromUnverfiedIdentities.length > 0) {
    if (categorized.selected) {
      // we already found a match from verified identities, discard all profiles from unverified identities
      categorized.discarded = profilesFromUnverfiedIdentities
    } else {
      const result = await findRelatedLinkedinProfilesWithLLM(
        memberId,
        memberData,
        profilesFromUnverfiedIdentities,
      )

      // check if empty object
      if (result.profileIndex !== null) {
        if (!categorized.selected) {
          categorized.selected = profilesFromUnverfiedIdentities[result.profileIndex]
        }
        // add profiles not selected to discarded
        for (let i = 0; i < profilesFromUnverfiedIdentities.length; i++) {
          if (i !== result.profileIndex) {
            categorized.discarded.push(profilesFromUnverfiedIdentities[i])
          }
        }
      } else {
        // if no match found, we should discard all profiles from verified identities
        categorized.discarded = profilesFromUnverfiedIdentities
      }
    }
  }

  return categorized
}

export async function squashWorkExperiencesWithLLM(
  memberId: string,
  workExperiencesFromMultipleSources: IMemberEnrichmentDataNormalizedOrganization[][],
): Promise<IMemberEnrichmentDataNormalizedOrganization[]> {
  const prompt = `
     
        ## INPUT
        ${JSON.stringify(workExperiencesFromMultipleSources)}
  
        ## INFORMATION
        You are given an input consisting of nested arrays of normalized organization data (IMemberEnrichmentDataNormalizedOrganization[][]). 
        Each data entry includes the following fields:
          name: The name of the organization.
          identities: An optional array of unique identities for the organization.
          title: An optional job title at the organization.
          organizationDescription: An optional description of the organization.
          startDate: An optional start date for the role (ISO format string).
          endDate: An optional end date for the role (ISO format string, or null if ongoing).
          source: The source of the organization data.
        
        ## OBJECTIVE
        Generate a single, chronologically ordered array of IMemberEnrichmentDataNormalizedOrganization objects that represents the most accurate work experience timeline.
  
        Guidelines:
          Order Chronologically:
            Sort the roles by startDate. If startDate is missing, infer the order based on available endDate or other contextual data.
          Merge Overlapping Roles IN DIFFERENT SOURCES:
            Never try merging roles from the same source.
            If multiple roles from the same organization overlap in time IN DIFFERENT SOURCES, squash them into one entry with a unified startDate, endDate, and picked information (e.g., job titles, descriptions).
            Preserve all unique identities and consolidate other fields appropriately.
            If necessary, ONLY merge dateRanges and NEVER merge titles together, but pick the one that best represents the role.
          Handle Missing Dates:
            Use logical assumptions to fill gaps where possible, always using existing date information but nothing else.
            If there is a role with a missing startDate and a missing endDate, and there's also another role from same or similar organization with dates, you can remove the role with missing dates.
          Prioritize Current Roles:
            Ongoing roles (endDate = null) should be placed last in the timeline.
          Ensure Accuracy:
            Maintain all relevant data fields in the final timeline and ensure no essential information is lost.
          
        Output Format:
        Return a single array of IMemberEnrichmentDataNormalizedOrganization objects:
  
        Input Example:
        [
          [
            {
              "name": "Company X",
              "title": "Developer",
              "startDate": "2020-01-01",
              "endDate": "2021-01-01",
              "source": "Resume"
            },
            {
              "name": "Company X",
              "title": "Senior Developer",
              "startDate": "2020-06-01",
              "endDate": "2021-12-31",
              "source": "LinkedIn"
            }
          ],
          [
            {
              "name": "Company Y",
              "title": "Manager",
              "startDate": "2022-01-01",
              "endDate": null,
              "source": "Manual Entry"
            }
          ]
        ]
  
        Output Example:
        [
          {
            "name": "Company X",
            "title": "Developer",
            "startDate": "2020-01-01",
            "endDate": "2020-06-01",
            "source": "Resume"
          },
          {
            "name": "Company X",
            "title": "Senior Developer",
            "startDate": "2020-06-01",
            "endDate": "2021-12-31",
            "source": "LinkedIn"
          }
          {
            "name": "Company Y",
            "title": "Manager",
            "startDate": "2022-01-01",
            "endDate": null,
            "source": "Manual Entry"
          }
        ]
  
        Ensure the response is a **valid and complete JSON**.
        DO NOT output anything else.
        Output ONLY valid JSON
    `

  const llmService = new LlmService(
    dbStoreQx(svc.postgres.writer),
    {
      accessKeyId: process.env['CROWD_AWS_BEDROCK_ACCESS_KEY_ID'],
      secretAccessKey: process.env['CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY'],
    },
    svc.log,
  )

  const result = await llmService.squashWorkExperiencesFromMultipleSources<
    IMemberEnrichmentDataNormalizedOrganization[]
  >(memberId, prompt)
  return result.result
}
