export function escapeGithubMarkdown(text: string): string {
  return text.replace(/[\\`*_[\]<>#~]/g, '\\$&').replace(/@/g, '&#64;')
}
