// Docker Hub repository slugs are lowercase and limited to [a-z0-9._-].
// GitHub allows uppercase and a few characters Hub rejects, so we lowercase
// and validate before probing — anything that fails the regex would 400 on
// Hub anyway and isn't worth an HTTP round-trip.
const HUB_COMPONENT = /^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$/

export function buildCandidates(owner: string, name: string): string[] {
  const ns = owner.toLowerCase()
  const repo = name.toLowerCase()

  if (!HUB_COMPONENT.test(ns) || !HUB_COMPONENT.test(repo)) {
    return []
  }

  // v1 deliberately omits `library/<repo>`: a random github.com/foo/node with a
  // dev Dockerfile would false-positive onto the official library/node image.
  // Official images (~150) to be seeded via allowlist in a follow-up.
  return [`${ns}/${repo}`]
}
