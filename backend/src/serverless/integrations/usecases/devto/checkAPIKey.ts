import axios from 'axios'

export const checkAPIKey = async (apiKey: string): Promise<boolean> => {
  try {
    const response = await axios.get('https://dev.to/api/articles/me', {
      headers: {
        Accept: 'application/vnd.forem.api-v1+json',
        'api-key': apiKey || '',
      },
    })

    return response.status === 200
  } catch {
    return false
  }
}
