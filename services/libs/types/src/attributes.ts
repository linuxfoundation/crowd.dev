export interface IAttributes {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: Record<string, any>
}

export interface IGeo {
  countryCode: string
  region?: string
  city?: string
}
