import axios from 'axios'
import mergeWith from 'lodash.mergewith'

import {
  MemberField,
  deleteMemberOrganizations,
  fetchMemberAttributes,
  fetchMemberIdentities,
  findMemberById,
  insertMemberBotSuggestion,
  insertMemberNoBot,
} from '@crowd/data-access-layer'
import { IDbMemberBotSuggestionInsert } from '@crowd/data-access-layer/src/members/types'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { IAttributes } from '@crowd/types'

import { svc } from '../../main'
import { MemberForLLMBotSuggestion } from '../../types/member'
import { sortMemberIdentities } from '../../utils'

export async function getMemberForBotAnalysis(
  memberId: string,
): Promise<MemberForLLMBotSuggestion | null> {
  try {
    const qx = pgpQx(svc.postgres.reader.connection())

    const [member, identities] = await Promise.all([
      findMemberById(qx, memberId, [MemberField.DISPLAY_NAME, MemberField.ATTRIBUTES]),
      fetchMemberIdentities(qx, memberId),
    ])

    if (!member) {
      return null
    }

    return {
      displayName: member.displayName,
      bio: member.attributes?.bio ?? null,
      identities: sortMemberIdentities(identities),
    }
  } catch (error) {
    svc.log.error({ error, memberId }, `Failed to find member!`)
    throw error
  }
}

export async function updateMemberAttributes(
  memberId: string,
  attributes: IAttributes,
): Promise<void> {
  const qx = pgpQx(svc.postgres.reader.connection())

  // PATCH endpoint requires full attributes, so we merge new ones with current
  const currentAttributes = await fetchMemberAttributes(qx, memberId)
  const attributesToUpdate = mergeWith({}, currentAttributes, attributes)

  const url = `${process.env['CROWD_API_SERVICE_URL']}/member/${memberId}/attributes`
  const requestOptions = {
    method: 'PATCH',
    headers: {
      Authorization: `Bearer ${process.env['CROWD_API_SERVICE_USER_TOKEN']}`,
      'Content-Type': 'application/json',
    },
    data: {
      ...attributesToUpdate,
    },
    params: {
      manuallyChanged: false,
    },
  }

  try {
    await axios(url, requestOptions)
  } catch (error) {
    svc.log.error(
      { error, memberId },
      `Failed updating member attributes with status [${error.response?.status}]`,
    )

    throw error
  }
}

export async function removeMemberOrganizations(memberId: string): Promise<void> {
  try {
    const qx = pgpQx(svc.postgres.writer.connection())
    await deleteMemberOrganizations(qx, memberId, undefined, true)
  } catch (error) {
    svc.log.error({ error, memberId }, `Failed to remove member organizations!`)
    throw error
  }
}

export async function createMemberBotSuggestion(
  suggestion: IDbMemberBotSuggestionInsert,
): Promise<void> {
  try {
    const qx = pgpQx(svc.postgres.writer.connection())
    await insertMemberBotSuggestion(qx, suggestion)
  } catch (error) {
    svc.log.error({ error, suggestion }, `Failed to create member bot suggestion!`)

    throw error
  }
}

export async function createMemberNoBot(memberId: string): Promise<void> {
  try {
    const qx = pgpQx(svc.postgres.writer.connection())
    await insertMemberNoBot(qx, memberId)
  } catch (error) {
    svc.log.error({ error, memberId }, `Failed to create member no bot!`)

    throw error
  }
}
