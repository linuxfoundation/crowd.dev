export interface ILocationCountry {
  location: string
  status: 'resolved' | 'ambiguous' | 'unknown'
  countryCode: string | null
}

export interface ICountryCandidate {
  id: string
  location: string
}

export interface ICountryUpdate {
  id: string
  source: string
  country: string
}

export interface IMemberCountryHints {
  orgCountries: string[]
  emailDomains: string[]
}

export interface ICountryScanCursor {
  id: string
  updatedAt: string
}

export interface ICountryScanWindow {
  since: string
  until: string
  after: ICountryScanCursor | null
}

export type ICountryScanCandidate = ICountryCandidate & ICountryScanCursor
