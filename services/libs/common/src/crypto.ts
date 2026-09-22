import * as crypto from 'crypto'

import config from 'config'

export interface EncryptionConfig {
  secretKey: string
  initVector: string
}

/**
 * Gets encryption config from config package
 * The config package looks for a 'config' directory relative to process.cwd()
 */
function getEncryptionConfig(): EncryptionConfig {
  if (!config.has('encryption')) {
    throw new Error('Encryption config not found in config package')
  }

  const encryptionConfig = config.get('encryption') as EncryptionConfig

  if (!encryptionConfig?.secretKey || !encryptionConfig?.initVector) {
    throw new Error('Encryption config must have secretKey and initVector')
  }

  return {
    secretKey: encryptionConfig.secretKey,
    initVector: encryptionConfig.initVector,
  }
}

/**
 * Encrypts data using AES-256-CBC algorithm
 * Reads encryption configuration from config package
 * @param password - The data to encrypt
 * @returns Encrypted hex string
 */
export function encryptData(password: string): string {
  const encryptionConfig = getEncryptionConfig()
  const algo = 'aes-256-cbc'
  const cipherText = crypto.createCipheriv(
    algo,
    encryptionConfig.secretKey,
    encryptionConfig.initVector,
  )

  let encryptedData = cipherText.update(password, 'utf-8', 'hex')
  encryptedData += cipherText.final('hex')

  return encryptedData
}

/**
 * Decrypts data using AES-256-CBC algorithm
 * Reads encryption configuration from config package
 * @param encryptedPassword - The encrypted hex string to decrypt
 * @returns Decrypted string
 */
export function decryptData(encryptedPassword: string): string {
  const encryptionConfig = getEncryptionConfig()
  const algo = 'aes-256-cbc'
  const decipherText = crypto.createDecipheriv(
    algo,
    encryptionConfig.secretKey,
    encryptionConfig.initVector,
  )

  let decryptedData = decipherText.update(encryptedPassword, 'hex', 'utf-8')
  decryptedData += decipherText.final('utf8')

  return decryptedData
}
