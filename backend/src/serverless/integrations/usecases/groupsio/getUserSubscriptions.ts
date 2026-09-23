import axios from 'axios'

import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('getGroupsIoUserSubscriptions')

export interface Subscription {
  id: number
  object: string
  created: string
  updated: string
  user_id: number
  group_id: number
  group_name: string
  nice_group_name: string
  status: string
  post_status: string
  email_delivery: string
  message_selection: string
  auto_follow_replies: boolean
  max_attachment_size: string
  approved_posts: number
  mod_status: string
  email: string
  user_status: string
  user_name: string
  timezone: string
  full_name: string
}

interface GetSubscriptionsResponse {
  object: string
  total_count: number
  start_item: number
  end_item: number
  has_more: boolean
  next_page_token?: number
  sort_field: string
  second_order: string
  query: string
  sort_dir: string
  data: Subscription[]
}

export const getUserSubscriptions = async (cookie: string): Promise<Subscription[]> => {
  let allSubscriptions: Subscription[] = []
  let nextPageToken: number | undefined

  do {
    const url = 'https://groups.io/api/v1/getsubs'
    const params = {
      limit: 100,
      ...(nextPageToken ? { page_token: nextPageToken } : {}),
    }

    try {
      const response = await axios.get<GetSubscriptionsResponse>(url, {
        params,
        headers: {
          Cookie: cookie,
        },
      })

      allSubscriptions = [...allSubscriptions, ...response.data.data]
      nextPageToken = response.data.next_page_token
    } catch (error) {
      log.error('Error fetching groups.io subscriptions:', error)
      throw error
    }
  } while (nextPageToken)

  return allSubscriptions
}
