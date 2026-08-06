import { IMemberEnrichmentLinkedinScraperMetadata } from '../../types'

export interface IMemberEnrichmentCrustdataEmployer {
  name?: string
  title?: string
  professional_network_id?: string
  start_date?: string
  end_date?: string
}

export interface IMemberEnrichmentCrustdataPersonData {
  basic_profile?: {
    name?: string
    current_title?: string
    headline?: string
    summary?: string
    languages?: string[]
    profile_picture_permalink?: string
  }
  social_handles?: {
    professional_network_identifier?: {
      profile_url?: string
    }
    twitter_identifier?: {
      slug?: string
    }
    dev_platform_identifier?: {
      profile_url?: string
    }
  }
  professional_network?: {
    connections?: number
    profile_picture_url?: string
    profile_picture_permalink?: string
  }
  skills?: {
    professional_network_skills?: string[]
  }
  education?: {
    schools?: Array<{
      school?: string
    }>
  }
  experience?: {
    employment_details?: {
      current?: IMemberEnrichmentCrustdataEmployer[]
      past?: IMemberEnrichmentCrustdataEmployer[]
    }
  }
}

export interface IMemberEnrichmentDataCrustdata extends IMemberEnrichmentCrustdataPersonData {
  metadata: IMemberEnrichmentLinkedinScraperMetadata
}

export interface IMemberEnrichmentCrustdataEnrichMatch {
  confidence_score: number
  person_data: IMemberEnrichmentCrustdataPersonData
}

export interface IMemberEnrichmentCrustdataEnrichResult {
  matched_on: string
  match_type: string
  matches: IMemberEnrichmentCrustdataEnrichMatch[]
}

export type IMemberEnrichmentCrustdataEnrichResponse = IMemberEnrichmentCrustdataEnrichResult[]

export interface IMemberEnrichmentCrustdataRemainingCredits {
  account: {
    credits: number
  }
}
