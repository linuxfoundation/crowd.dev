/* eslint-disable @typescript-eslint/no-explicit-any */
import cloneDeep from 'lodash.clonedeep'
import merge from 'lodash.merge'

import { getServiceChildLogger } from '@crowd/logging'
import {
  ActivityDisplayVariant,
  ActivityTypeDisplayProperties,
  ActivityTypeSettings,
  PlatformType,
} from '@crowd/types'

import { UNKNOWN_ACTIVITY_TYPE_DISPLAY } from './activityTypes'
import { DiscordActivityType } from './discord/types'

const log = getServiceChildLogger('ActivityDisplayService')

export class ActivityDisplayService {
  static getInterpolatableVariables(
    string: string,
    interpolatableVariables: string[] = [],
  ): string[] {
    const interpolationStartIndex = string.indexOf('{')
    const interpolationEndIndex = string.indexOf('}')

    // we don't need processing if there's no opening/closing brackets, or when the string is empty
    if (interpolationStartIndex === -1 || interpolationEndIndex === -1 || string.length === 0) {
      return interpolatableVariables
    }

    const interpolationVariable = string.slice(interpolationStartIndex + 1, interpolationEndIndex)
    interpolatableVariables.push(interpolationVariable)

    return this.getInterpolatableVariables(
      string.slice(interpolationEndIndex + 1),
      interpolatableVariables,
    )
  }

  static interpolateVariables(
    displayOptions: ActivityTypeDisplayProperties,
    activity: any,
    selectedDisplayVariants: string[],
  ): ActivityTypeDisplayProperties {
    for (const key of Object.keys(displayOptions)) {
      if (typeof displayOptions[key] === 'string' && selectedDisplayVariants.includes(key)) {
        const displayVariables = this.getInterpolatableVariables(displayOptions[key])
        for (const dv of displayVariables) {
          const coalesceVariables = dv.split('|')
          let replacement = ''

          for (const variable of coalesceVariables) {
            const attribute = this.getAttribute(variable.trim(), activity)

            if (attribute) {
              replacement = attribute
              break
            }
          }

          if (displayOptions.formatter && displayOptions.formatter[dv]) {
            replacement = displayOptions.formatter[dv](replacement)
          }
          displayOptions[key] = displayOptions[key].replace(`{${dv}}`, replacement)
        }
      }
    }

    return displayOptions
  }

  static getAttribute(key: string, activity: any) {
    if (key === 'self') {
      return activity
    }

    const splitted = key.split('.')

    let attribute = activity

    for (const key of splitted) {
      try {
        attribute = attribute[key]
      } catch {
        return null
      }
    }

    return attribute
  }

  static getDisplayOptions(
    activity: any,
    activityTypes: ActivityTypeSettings,
    selectedDisplayVariants: ActivityDisplayVariant[] = [
      ActivityDisplayVariant.DEFAULT,
      ActivityDisplayVariant.SHORT,
      ActivityDisplayVariant.CHANNEL,
    ],
  ): ActivityTypeDisplayProperties {
    try {
      if (!activity || !activity.platform || !activity.type) {
        return UNKNOWN_ACTIVITY_TYPE_DISPLAY
      }

      const allActivityTypes = merge(activityTypes.custom, activityTypes.default)

      if (
        activity.platform === PlatformType.DISCORD &&
        activity.type === DiscordActivityType.MESSAGE &&
        activity.attributes &&
        activity.attributes.thread === true
      ) {
        activity.type = DiscordActivityType.THREAD_MESSAGE
      }

      // we're cloning because we'll use the same object to do the interpolation
      const displayOptions: ActivityTypeDisplayProperties =
        allActivityTypes[activity.platform] && allActivityTypes[activity.platform][activity.type]
          ? cloneDeep(allActivityTypes[activity.platform][activity.type].display)
          : null

      if (!displayOptions) {
        // return default display
        return UNKNOWN_ACTIVITY_TYPE_DISPLAY
      }

      return this.interpolateVariables(displayOptions, activity, selectedDisplayVariants)
    } catch (error) {
      console.log(error)
      log.debug(
        { error },
        'Error while getting display options, falling back to UNKNOWN_ACTIVITY_TYPE_DISPLAY.',
      )
      return UNKNOWN_ACTIVITY_TYPE_DISPLAY
    }
  }
}
