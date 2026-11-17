export type Logger = {
  info(msg: string): void
  warning(msg: string): void
  error(msg: string): void
}

/** Writes plain lines and GitHub Actions workflow commands to stdout. */
export const consoleLogger: Logger = {
  info: (msg) => console.log(msg),
  warning: (msg) => console.log(`::warning::${msg}`),
  error: (msg) => console.log(`::error::${msg}`),
}
