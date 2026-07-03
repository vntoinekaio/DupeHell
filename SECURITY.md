# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.x     | :white_check_mark: |

## Reporting a Vulnerability

DupeHell generates **synthetic data** and does not process real personally identifiable information. However, security issues related to the generation pipeline, dependency vulnerabilities, or potential misuse vectors should be reported.

If you discover a security vulnerability, please **do not open a public issue**. Instead, report it privately via GitHub's Security Advisory tab:

https://github.com/anomalyco/dupehell/security/advisories

You can also contact the maintainers directly through GitHub.

### What to include

- Description of the vulnerability
- Steps to reproduce (if applicable)
- Potential impact
- Suggested fix (optional)

### Response timeline

- **Acknowledgment** within 48 hours
- **Initial assessment** within 5 business days
- **Fix timeline** communicated based on severity

## Scope

- Generation pipeline code (Python + Rust)
- CLI argument handling
- Pool data files
- Dependencies (Polars, NumPy, PyArrow, Textual, etc.)

## Out of scope

- Misuse of the generated synthetic data (covered by ETHICS.md)
- Hypothetical attacks requiring physical access or modified runtime environments

## Responsible Disclosure

Please allow reasonable time for a fix before any public disclosure. We will credit reporters in release notes unless they prefer to remain anonymous.
