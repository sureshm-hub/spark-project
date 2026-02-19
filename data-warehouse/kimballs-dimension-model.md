# Kimball's logical design (Facts/Dimensions/Conformed Dimensions)
- remains a foundational approach to data warehousing designed for **intuitive business querying and high 
  performance.** 
- It revolves around modeling business processes using a "star schema," which separates data into two types of tables: 
  - Facts (measurable, quantitative events) and 
  - Dimensions (descriptive, context-providing attributes)

## Core Components (The "What")
- **Fact Tables:** Contain foreign keys to dimensions and numerical measures (e.g., sales_amount, quantity_sold).
- **Dimension Tables:** Contain descriptive attributes (e.g., product_name, customer_segment).
- **Star Schema:** A central fact table surrounded by denormalized dimension tables, optimizing for read-heavy 
  analytics.
- **Surrogate Keys:** Artificial, non-natural keys used in dimension tables to track changes over time, regardless of 
  source system keys. 

## Important Points Still in Use Today
- **Conformed Dimensions (The "Bus Matrix"):**
**_Why it survives:_** Ensures consistency across the enterprise. A Customer dimension used by Sales should be the same 
as the one used by Marketing.
**_Modern usage:_** Implemented in dbt (data build tool) as shared, upstream models that ensure a single source of truth.

- **Explicit Grain Declaration**
**_Why it survives:_** "One row equals one ____." Without defining the grain, dashboards show conflicting numbers.
**_Modern usage:_** Documented in dbt metadata to prevent confusion on whether a fact represents a transaction, a line 
   item, or a daily snapshot.

- **Slowly Changing Dimensions (SCD Type 2)**
**_Why it survives:_** Business needs to track history (e.g., "What region did this customer belong to when they bought 
  this?").
**_Modern usage:_** Handled by dbt snapshots, which automate tracking changes in dimension rows.

- **Business Process Focus**
**_Why it survives:_** You build data models for activities (e.g., "Order") rather than source systems (e.g., "SAP").

- **Surrogate Keys (or Hash Keys)**
**_Why it survives:_** Essential for SCD Type 2 and managing data integration from multiple sources.
**_Modern usage:_** Often replaced by Hash Keys (using md5/sha256) in modern data platforms to make ETL/ELT faster and 
more deterministic.

## Modern Adaptations of Kimball (2025-2026)
- While the theory holds, the **_implementation has evolved:_**
- **From ETL to ELT:** Instead of complex pre-processing, data is loaded raw and transformed using SQL in tools like 
  dbt to create the star schema.
- **Hybrid Models:** Often, data is staged using Kimball principles, then materialized into "One Big Table" (OBT) for 
  maximum performance in columnar databases (e.g., BigQuery, Redshift).
- **Denormalization:** With modern storage costs being low, dimensions are often heavily flattened to reduce joins.

## Bus Matrix
- A **bus matrix** is a Kimball data warehousing architectural tool that maps business processes (rows) to conformed
  dimensions (columns) to ensure data consistency, integration, and reusability across an organization.
  
- **Key Components and Purpose:**
  - **Rows (Business Processes):** Specific activities like "Sales," "Inventory," or "Marketing".
  - **Columns (Conformed Dimensions):** Shared, standardized data attributes like "Date," "Customer," or "Product". 
  - **Purpose:** Ensures that a dimension (e.g., "Customer") means the same thing across all business processes. 
  - **Benefits:** Prevents data silos, reduces ETL complexity, and provides a roadmap for incremental data warehouse 
    development.

- **How to Build a Bus Matrix:**
  - **Identify Business Processes:** List key events (rows).
  - **Define Dimensions:** Identify the who, what, where, when, and why (columns).
  - **Map Relationships:** Mark where a dimension applies to a process.
  - **Confirm Dimensions:** Ensure shared dimensions are standardized. 

## Data Mart
- In Kimball dimensional modeling, a data mart is a subject-oriented, specialized collection of fact and dimension 
  tables (often a star schema) representing a specific business process, such as sales, inventory, or billing.
- They act as the foundational, user-centric building blocks for an enterprise data warehouse. 
- A data warehouse is a large, centralized repository storing enterprise-wide data from multiple sources for 
  comprehensive analysis, while a data mart is a subset focused on a specific department, team, or subject area
- **Bottom-Up Approach:** Data warehouses are built by integrating these individual data marts via the "Bus 
  Architecture" rather than building a monolithic warehouse first.
- **Conformed Dimensions:** Data marts are not isolated; they use shared, standardized dimensions (e.g., "Date," 
  "Customer") to allow for consistent, cross-functional reporting across different departments.
- **Business Process Orientation:** Unlike department-focused silos, Kimball marts are modeled around business 
  processes (e.g., "Order Line Items").
- **Star Schema Focus:** Each mart typically consists of a single central fact table surrounded by denormalized 
  dimension tables designed for high-performance querying.