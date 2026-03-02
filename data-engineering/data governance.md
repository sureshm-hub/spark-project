# Data Governance Practices and Projects

## Domain Driven Data governance:
https://medium.com/zs-associates/domain-driven-data-governance-what-it-is-how-it-works-and-why-it-creates-value-6ac5dc969496

## 1. Core Data Governance Practices

- Define data ownership and stewardship (business data owners, technical custodians, and stewards per domain).
- Maintain a business glossary and data dictionary for key metrics and entities.
- Establish data quality rules and SLAs (completeness, accuracy, timeliness, validity, uniqueness).
- Implement access control and classification (public, internal, confidential, restricted) with role-based access.
- Set standards for metadata and lineage capture (where data comes from, how it is transformed, where it is used).
- Create policies for privacy, retention, and regulatory compliance (for example, GDPR, CCPA, SOX, HIPAA).
- Introduce change management for schemas and data contracts between producers and consumers.
- Define a governance operating model (council, working groups, domain forums, RACI, escalation paths).
- Run continuous data literacy and training programs for stakeholders.
- Track governance KPIs (data quality scores, policy violations, incident counts, certified datasets).

## 2. Example Governance Projects

- Stand up a central data catalog with certified “gold” datasets and documented ownership.
- Create an enterprise business glossary for all critical risk and finance metrics.
- Implement data quality monitoring on key regulatory or risk-reporting tables.
- Roll out role-based access controls and masking for sensitive or personal data across the data lake or warehouse.
- Build end-to-end data lineage for a high-stakes report (for example, liquidity, capital, CCAR, or regulatory filings).
- Launch a data contract program between source systems and downstream analytics teams.
- Run a data domain pilot (for example, “Customer” or “Product”) to define owners, policies, and quality dashboards.
- Execute a data retention and archival project to enforce legal hold and purge policies.
- Integrate data governance checks into CI/CD for ETL and ELT pipelines (schema drift, quality gates).
- Start a data literacy initiative (office hours, “data university,” documentation sprints) to drive adoption.