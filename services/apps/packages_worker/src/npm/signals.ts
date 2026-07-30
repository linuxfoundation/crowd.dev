// AbortSignal.any() isn't in this @types/node version yet — combine manually so an
// external signal (e.g. a Temporal activity's cancellationSignal) can abort the
// request early, alongside the request's own internal timeout.
export function combineSignals(internal: AbortSignal, external?: AbortSignal): AbortSignal {
  if (!external) return internal
  if (external.aborted) return external
  const controller = new AbortController()
  internal.addEventListener('abort', () => controller.abort(internal.reason), { once: true })
  external.addEventListener('abort', () => controller.abort(external.reason), { once: true })
  return controller.signal
}
