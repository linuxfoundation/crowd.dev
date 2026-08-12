import { getCountry, groupBy } from '@crowd/common'
import { OrganizationAttributeSource, OrganizationAttributeType } from '@crowd/types'

import {
  ORG_DB_ATTRIBUTES,
  ORG_DB_ATTRIBUTE_SOURCE_PRIORITY,
  ORG_DB_FIELDS,
} from './attributesConfig'
import {
  IDbOrgAttribute,
  IDbOrgAttributeInput,
  IDbOrganization,
  IDbOrganizationInput,
} from './types'

/* eslint-disable @typescript-eslint/no-explicit-any */

export interface IPrepareOrgResult {
  organization: IDbOrganizationInput
  attributes: IDbOrgAttributeInput[]
}

export function prepareOrganizationData(
  incoming: any,
  source: string,
  existingOrg?: IDbOrganization,
  existingAttributes?: IDbOrgAttribute[],
): IPrepareOrgResult {
  // validate source
  if (!ORG_DB_ATTRIBUTE_SOURCE_PRIORITY.includes(source as OrganizationAttributeSource)) {
    throw new Error(
      `Invalid attribute source: ${source} - must be one of ${ORG_DB_ATTRIBUTE_SOURCE_PRIORITY.join(
        ', ',
      )}`,
    )
  }

  const attributeSource = source as OrganizationAttributeSource
  const incomingFields = Object.keys(incoming)

  // process incoming fields
  const orgRes: any = {}
  const attributes: IDbOrgAttributeInput[] = []

  // process regular fields
  // default attribute fields will also be processed here
  // but will possibly be overriden later with default values calculated by priority
  for (const field of incomingFields.filter((k) => ORG_DB_FIELDS.includes(k))) {
    // set the field only if it's not already set
    if (
      incoming[field] !== undefined &&
      (existingOrg === undefined || existingOrg[field] === undefined)
    ) {
      orgRes[field] = incoming[field]
    }
  }

  // generate attributes
  for (const attDef of incomingFields
    .filter((k) => ORG_DB_ATTRIBUTES.some((a) => a.name === k))
    .map((k) => ORG_DB_ATTRIBUTES.find((a) => a.name === k))) {
    const data = incoming[attDef.name]
    if (data === undefined || data === null) {
      continue
    }

    const values: string[] = []
    let defaultSource: OrganizationAttributeSource
    let attributesBySource: Map<OrganizationAttributeSource, IDbOrgAttribute[]>

    if (attDef.incomingType === 'string_array') {
      if (!Array.isArray(data)) {
        throw new Error(`Expected ${attDef.name} to be an array`)
      }

      if (data.length === 0) {
        continue
      }

      // generate attribute for each value in the array
      values.push(...data)
    } else if (attDef.incomingType === 'object_array') {
      if (!Array.isArray(data)) {
        throw new Error(`Expected ${attDef.name} to be an array`)
      }

      if (data.length === 0) {
        continue
      }

      // stringify the value
      values.push(JSON.stringify(data))
    } else if (attDef.incomingType === OrganizationAttributeType.OBJECT) {
      // stringify the value
      values.push(JSON.stringify(data))
    } else {
      // primitive value
      values.push(String(data))
    }

    // sometimes an attribute value can be just an empty array
    if (values.length === 0) {
      continue
    }

    const existing = (existingAttributes || []).filter(
      (a) => a.name === attDef.name && a.source !== attributeSource,
    )

    if (existing.length > 0) {
      // complicated - we need to take into account existing attributes and their sources when calculating default value
      attributesBySource = groupBy(existing, (e) => e.source) as Map<
        OrganizationAttributeSource,
        IDbOrgAttribute[]
      >

      // lets find the default source that we should use to set the default value
      for (const prioritySource of ORG_DB_ATTRIBUTE_SOURCE_PRIORITY) {
        if (attributesBySource.has(prioritySource)) {
          defaultSource = prioritySource
          break
        }

        // If we've reached the incoming source priority, use it since no higher priority exists
        if (prioritySource === attributeSource) {
          defaultSource = attributeSource
          break
        }
      }

      // we should always have defaultSource calculated
      if (!defaultSource) {
        throw new Error(
          `Could not find default source! Is ${attributeSource} not in the priority list? [${ORG_DB_ATTRIBUTE_SOURCE_PRIORITY.join(
            ', ',
          )}]`,
        )
      }

      // now we just add new attributes to the attributesBySource map
      attributesBySource.set(
        attributeSource,
        values.map((v) => {
          return {
            type: attDef.type,
            name: attDef.name,
            source: attributeSource,
            default: false,
            value: v,
          }
        }),
      )
    } else {
      // simple - just create a new attribute
      const newAttributes = []
      for (const value of values) {
        newAttributes.push({
          type: attDef.type,
          name: attDef.name,
          source: attributeSource,
          default: false,
          value,
        })
      }

      attributesBySource = new Map()
      attributesBySource.set(attributeSource, newAttributes)
      defaultSource = attributeSource
    }

    // we set default: false for all attributes that have source different from defaultSource
    for (const nonDefaultSource of Array.from(attributesBySource.keys()).filter(
      (s) => s !== defaultSource,
    )) {
      for (const att of attributesBySource.get(nonDefaultSource) || []) {
        att.default = false
      }
    }

    // we set default values now
    let set = false
    for (const att of attributesBySource.get(defaultSource) || []) {
      // if attribute has multiple values (is an array) we just pick the first one for the default value
      if (!set) {
        att.default = true
        set = true

        // we also set the org field if it's a default column
        if (
          attDef.defaultColumn &&
          existingOrg &&
          (existingOrg[attDef.defaultColumn] === undefined ||
            existingOrg[attDef.defaultColumn] === null ||
            existingOrg[attDef.defaultColumn] !== att.value)
        ) {
          if (attDef.type === OrganizationAttributeType.OBJECT) {
            orgRes[attDef.defaultColumn] = JSON.parse(att.value)
          } else if (
            attDef.type === OrganizationAttributeType.DECIMAL ||
            attDef.type === OrganizationAttributeType.INTEGER
          ) {
            orgRes[attDef.defaultColumn] = Number(att.value)
          } else if (attDef.type === OrganizationAttributeType.BOOLEAN) {
            orgRes[attDef.defaultColumn] = att.value === 'true'
          } else {
            orgRes[attDef.defaultColumn] = att.value
          }
        }
      } else {
        att.default = false
      }
    }

    for (const atts of Array.from(attributesBySource.values())) {
      attributes.push(...atts)
    }
  }

  // Infer country from location when missing
  if (
    !attributes.some(({ name }) => name === 'country') &&
    !(existingAttributes ?? []).some(({ name }) => name === 'country')
  ) {
    const location =
      attributes.find(({ name, default: isDefault }) => name === 'location' && isDefault)?.value ??
      (existingAttributes ?? []).find(
        ({ name, default: isDefault }) => name === 'location' && isDefault,
      )?.value ??
      existingOrg?.location

    const country = getCountry(location)

    if (country) {
      attributes.push({
        name: 'country',
        source: OrganizationAttributeSource.SYSTEM,
        default: true,
        value: country,
      })

      orgRes.country = country
    }
  }

  return {
    organization: orgRes,
    attributes,
  }
}
