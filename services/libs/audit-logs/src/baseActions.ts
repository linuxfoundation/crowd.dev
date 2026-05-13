import { AuditLogAction } from '@crowd/data-access-layer'

export type CaptureValueFn = <T>(value: T) => T

export type CaptureFn<T> = (
  captureOldState: CaptureValueFn,
  captureNewState: CaptureValueFn,
) => Promise<T>

export type CaptureOneFn<T> = (captureState: CaptureValueFn) => Promise<T>

export type BuildActionFn<T> = () => Promise<{
  result: T
  auditLog: AuditLogAction
  error?: Error
}>

export function createCaptureFn(): { value: object; fn: CaptureValueFn } {
  const result = {
    value: {},
    fn: null,
  }

  result.fn = (newValue: object) => {
    result.value = newValue
    return newValue
  }

  return result
}
