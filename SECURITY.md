# Security Policy

To report a security vulnerability, please use GitHub's feature for private security reporting.

For more information, please read: [How to report vulnerabilities to LF projects and foundations](https://www.linuxfoundation.org/security).

## Vulnerability handling

Fix targets:

- Critical: 7 days.
- High: 30 days.
- Medium and low: best effort, reviewed monthly.

The GitHub Security tab is the source of truth. It covers Dependabot, code scanning and secret scanning. We do not open one issue per alert.

Fix work is tracked as `security` issues in linuxfoundation/lfx-cdp-insights-ops, grouped by package or root cause.

A weekly security report (`.github/workflows/security-report.yml`) lists open and overdue alerts.

Security updates wait out pnpm's minimum release age. A critical fix can get a manual exemption for one package.
