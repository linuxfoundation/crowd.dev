import cloneDeep from 'lodash.clonedeep'
import ldGet from 'lodash.get'
import isArray from 'lodash.isarray'
import isEqual from 'lodash.isequal'
import isPlainObject from 'lodash.isplainobject'
import mergeWith from 'lodash.mergewith'
import pick from 'lodash.pick'
import ldSet from 'lodash.set'
import unionWith from 'lodash.unionwith'

/* eslint-disable @typescript-eslint/no-explicit-any */

export const isObject = (val: unknown): boolean => {
  return val !== null && typeof val === 'object'
}

export const isNullOrUndefined = (val: unknown): boolean => {
  return val === null || val === undefined
}

export const isObjectEmpty = (val: unknown): boolean => {
  if (!isObject(val)) {
    return false
  }

  for (const key of Object.keys(val)) {
    const value = val[key]
    if (value !== undefined) {
      return false
    }
  }

  return true
}

export const mergeIgnoreUndefined = <T extends Record<string, unknown>>(first: T, second: T): T => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result: any = { ...first }

  for (const key of Object.keys(second)) {
    const value = second[key]
    if (value !== undefined) {
      result[key] = value
    }
  }

  return result
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type RemapT = { [index: string]: any }

export const renameKeys = <T extends RemapT>(obj: RemapT, fieldMap: RemapT): T =>
  Object.keys(obj).reduce(
    (acc, key) => ({
      ...acc,
      [fieldMap[key] || key]: obj[key],
    }),
    {} as T,
  )

/**
 * Given two object, perform a deepmerge. Then check if the deepMerged object
 * has any fields that are not in the original object.
 * Return an object with the merged fields that are different from the original object
 * @param {Object} originalObject
 * @param {Object} newObject
 * @param {Object} special: Fields that need special merging {field: (oldValue, newValue) => newValue}
 *  - To keep the earliest joinedAt:
 *      {joinedAt: (oldDate, newDate) => new Date(Math.min(newDate, oldDate))}
 *  - To always keep the original displayName:
 *      {displayName: (oldUsername, newUsername) => oldUsername}
 * @returns {Object} fields of the mergedObejct that are different from originalObject
 */
export const mergeObjects = (originalObject, newObject, special = {}) => {
  const originalCopy = cloneDeep(originalObject)
  const merged = mergeWith({}, originalObject, newObject, customizer)

  for (const key in special) {
    if (Object.prototype.hasOwnProperty.call(special, key)) {
      ldSet(merged, key, special[key](ldGet(originalObject, key), ldGet(merged, key)))
    }
  }

  return difference(merged, originalCopy)
}

/**
 * Customising the merge.
 * - If the field is an array, concatenate the arrays
 * - If the newField is null, keep the original field
 * - Else, return undefined, and lodash will do standard merge
 * @param originalField Original field
 * @param newField New field
 * @returns Customisations for the merge.
 */
function customizer(originalField, newField) {
  if (isArray(originalField)) {
    return unionWith(originalField, newField, isEqual)
  }
  if (newField == null) {
    return originalField
  }

  return undefined
}

/**
 * Deep diff between two object, using lodash
 * @param  {Object} newObject  Object obtained after loadash merge
 * @param  {Object} original   Original object
 * @return {Object}            Object with the fields from newObject that are different from original
 */
function difference(newObject, original) {
  const differentFields = Object.keys(newObject)
    .map((field) =>
      isEqual(newObject[field], original[field]) || newObject[field] === null ? null : field,
    )
    .filter((field) => field !== null)
  return pick(newObject, differentFields)
}

/**
 * Merges two objects, preserving non-null values in the original object.
 *
 * @param originalObject - The original object
 * @param newObject - The object to merge into the original
 * @returns The merged object
 */
export const safeObjectMerge = (originalObject: any, newObject: any): any => {
  const mergeCustomizer = (originalValue, newValue) => {
    // Merge arrays, removing duplicates
    if (isArray(originalValue)) {
      return unionWith(originalValue, newValue, isEqual)
    }

    // Recursively merge nested objects
    if (isPlainObject(originalValue)) {
      return mergeWith({}, originalValue, newValue, mergeCustomizer)
    }

    // Preserve original non-null or non-empty values
    if (newValue === null || (originalValue !== null && originalValue !== '')) {
      return originalValue
    }

    return undefined
  }

  return mergeWith({}, originalObject, newObject, mergeCustomizer)
}
