import { describe, expect, it } from 'vitest'

import { buildSkippedDiscussionAlert } from './skippedRequestAlert'

describe('buildSkippedDiscussionAlert', () => {
  it('links the source discussion and includes the skip reason', () => {
    const sections = buildSkippedDiscussionAlert(
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/123',
      },
      'repo already in CDP',
    )

    const summary = sections[0].text
    expect(summary).toContain('obmondo/kubeaid-cli')
    expect(summary).toContain('https://github.com/obmondo/kubeaid-cli')
    expect(summary).toContain('https://github.com/linuxfoundation/insights/discussions/123')

    const reason = sections[1]
    expect(reason.title).toBe('Skip reason')
    expect(reason.text).toContain('```')
    expect(reason.text).toContain('repo already in CDP')
  })

  it('falls back to a not-recorded note when sourceUrl is null', () => {
    const sections = buildSkippedDiscussionAlert(
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        sourceUrl: null,
      },
      'not a fit for LFX Insights',
    )

    expect(sections[0].text).toContain('source discussion not recorded')
  })

  it('truncates a very long skip reason', () => {
    const longReason = 'x'.repeat(600)

    const sections = buildSkippedDiscussionAlert(
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        sourceUrl: null,
      },
      longReason,
    )

    const reasonText = sections[1].text
    expect(reasonText).toContain('…')
    expect(reasonText.length).toBeLessThan(longReason.length)
  })

  it('labels a slack-bot request as a Slack message', () => {
    const [linked] = buildSkippedDiscussionAlert(
      {
        repoName: 'foo/bar',
        repoUrl: 'https://github.com/foo/bar',
        sourceUrl: 'https://acme.slack.com/archives/C1/p1',
        provenance: 'slack-bot',
      },
      'r',
    )
    const [unlinked] = buildSkippedDiscussionAlert(
      {
        repoName: 'foo/bar',
        repoUrl: 'https://github.com/foo/bar',
        sourceUrl: null,
        provenance: 'slack-bot',
      },
      'r',
    )

    expect(linked.text).toContain('<https://acme.slack.com/archives/C1/p1|Slack request>')
    expect(unlinked.text).toContain('source Slack message not recorded')
  })

  it('does not present a retained non-Slack source url as a Slack request', () => {
    const [header] = buildSkippedDiscussionAlert(
      {
        repoName: 'foo/bar',
        repoUrl: 'https://github.com/foo/bar',
        sourceUrl: 'https://github.com/foo/bar/discussions/1',
        provenance: 'slack-bot',
      },
      'r',
    )

    expect(header.text).toContain('source Slack message not recorded')
    expect(header.text).not.toContain('discussions/1')
  })
})
