export function parseEnvInt(
  value: string | undefined,
  defaultValue: number,
  min: number,
  max: number,
): number {
  const parsed = parseInt(value ?? '', 10)
  return Number.isFinite(parsed) && parsed >= min && parsed <= max ? parsed : defaultValue
}

// Max new (not-yet-in-projectCatalog) rows a single source can add per discovery run.
export const DISCOVERY_NEW_PROJECTS_LIMIT = parseEnvInt(
  process.env.CROWD_DISCOVERY_NEW_PROJECTS_LIMIT,
  20,
  1,
  10_000,
)
