<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses. -->

# Security Policy

---

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.x | ✅ |

---

## Reporting a vulnerability

DupeHell generates **synthetic data** and does not process real personally
identifiable information. However, security issues related to the generation
pipeline, dependency vulnerabilities, or potential misuse vectors should be
reported.

If you discover a security vulnerability, **do not open a public issue**. Report
it privately via GitHub's Security Advisory tab:

[https://github.com/vntoinekaio/DupeHell/security/advisories](https://github.com/vntoinekaio/DupeHell/security/advisories)

You can also contact the maintainers directly through GitHub.

### What to include

- Description of the vulnerability
- Steps to reproduce (if applicable)
- Potential impact
- Suggested fix (optional)

### Response timeline

| Step | Timeframe |
|------|-----------|
| Acknowledgment | Within 48 hours |
| Initial assessment | Within 5 business days |
| Fix timeline | Communicated based on severity |

---

## Scope

- Generation pipeline code (Python + Rust)
- CLI argument handling
- Pool data files
- Dependencies (arrow-rs, clap, serde, etc.)

## Out of scope

- Misuse of the generated synthetic data (covered by [ETHICS.md](../ETHICS.md))
- Hypothetical attacks requiring physical access or modified runtime environments

---

## Responsible disclosure

Please allow reasonable time for a fix before any public disclosure. We will
credit reporters in release notes unless they prefer to remain anonymous.