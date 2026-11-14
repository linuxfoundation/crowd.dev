export function describeChannelError(err: unknown): string {
  if (err instanceof Error) {
    return err.message
  }
  if (typeof err === 'string') {
    return err
  }
  try {
    return JSON.stringify(err) ?? String(err)
  } catch {
    return String(err)
  }
}
