import { decryptData } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'
import { PlatformType } from '@crowd/types'

export class CommonIntegrationService {
  private static readonly log = getServiceChildLogger('CommonIntegrationService')

  /**
   * Safely decrypts an encrypted value, returning the original value if decryption fails
   */
  private static safeDecrypt(encryptedValue: string): string {
    try {
      return decryptData(encryptedValue)
    } catch (error) {
      CommonIntegrationService.log.warn(`Failed to decrypt value: ${error?.message || error}`)
      return encryptedValue
    }
  }

  /**
   * Decrypts encrypted settings for integrations based on platform type
   * @param platform - The integration platform (e.g., CONFLUENCE, JIRA)
   * @param settings - The settings object that may contain encrypted fields
   * @returns Settings object with decrypted values
   */
  public static decryptIntegrationSettings(platform: string, settings) {
    if (!settings) return settings

    switch (platform) {
      case PlatformType.CONFLUENCE:
        return {
          ...settings,
          apiToken: settings.apiToken
            ? CommonIntegrationService.safeDecrypt(settings.apiToken)
            : settings.apiToken,
          orgAdminApiToken: settings.orgAdminApiToken
            ? CommonIntegrationService.safeDecrypt(settings.orgAdminApiToken)
            : settings.orgAdminApiToken,
        }
      case PlatformType.JIRA:
        if (settings.auth) {
          return {
            ...settings,
            auth: {
              ...settings.auth,
              personalAccessToken: settings.auth.personalAccessToken
                ? CommonIntegrationService.safeDecrypt(settings.auth.personalAccessToken)
                : settings.auth.personalAccessToken,
              apiToken: settings.auth.apiToken
                ? CommonIntegrationService.safeDecrypt(settings.auth.apiToken)
                : settings.auth.apiToken,
            },
          }
        }
        return settings
      default:
        return settings
    }
  }
}
