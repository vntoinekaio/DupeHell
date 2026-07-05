<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses. -->

# Ethical Use & Responsible Disclosure

## Educational & research purpose only

DupeHell is designed **solely for educational and research purposes** —
specifically for benchmarking and evaluating record linkage (entity resolution)
algorithms in a controlled environment.

This tool generates **synthetic data** that does not correspond to any real
individuals, organizations, or entities. No real personally identifiable
information (PII) is used, generated, or distributed by this project.

---

## Prohibited uses

You may **not** use DupeHell for any of the following:

- Generating data to **defraud, deceive, or harm** any individual or organization
- Creating synthetic identities for **identity theft, fraud, or impersonation**
- Generating datasets that **mimic or target** specific real individuals or
  organizations
- Any use that violates applicable **local, national, or international laws**
- **Military or surveillance** applications targeting civilians
- Generating data for **spam, phishing, or social engineering** campaigns

---

## No liability

The DupeHell authors and contributors **shall not be held liable** for any
damages arising from the use or misuse of this software. The software is
provided "as is," without warranty of any kind, express or implied.

Users assume full responsibility for ensuring their use complies with all
applicable laws and regulations in their jurisdiction.

---

## Responsible disclosure

If you discover a potential misuse vector in this project's design (e.g. a
way the tool could be abused beyond its intended scope), please report it
responsibly rather than disclosing it publicly first.

For actual **security vulnerabilities** (code-level issues, dependency CVEs,
etc.), do not open a public issue — see [SECURITY.md](docs/SECURITY.md) for
the private reporting process.

---

## Privacy

DupeHell does not collect, transmit, or store any user data. All generation
happens locally on your machine. No telemetry, analytics, or usage tracking is
included.

---

## What DupeHell is NOT

- **Not** a tool for generating realistic PII for any real-world application
- **Not** a substitute for real-world data evaluation — synthetic benchmarks measure algorithmic ceilings, not production readiness
- **Not** suitable for training production ML models — synthetic distributions do not generalize to real record linkage scenarios
- **Not** a compliance solution — it does not generate data that satisfies any privacy regulation (GDPR, CCPA, etc.)

**Not** a source of demographic or identity-coded data — pool files in
`assets/pools/` are intentionally **domain-agnostic**: no pool is segmented
by ethnicity, nationality, religion, or any other protected category, and
names are mixed across languages and locales. Do not use them to construct
datasets targeting protected categories. See
[assets/pools/README.md](assets/pools/README.md) for the full data design
principles.

---

## Attribution

If you use DupeHell in your research, please cite the project:

```bibtex
@software{dupehell2026,
  author = {DupeHell Contributors},
  title = {DupeHell: Synthetic Multi-Domain Dataset Generator for
           Record Linkage Benchmarking},
  year = {2026},
  url = {https://github.com/vntoinekaio/DupeHell}
}
```

---

*This document reflects the ethical stance of the DupeHell project and may be
updated as the project evolves.*