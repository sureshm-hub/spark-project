# SCD

- handles dimension data changes only (not fact table)

## different types of slowly changing dimensions
- Type 0: ***Fixed*** attributes
- Type 1: ***Overwrite*** old values
- Type 2: Track full history with ***new rows**
- Type 3: Store limited history with ***extra columns***
- Type 4: Separate ***history table***

# SCD in Stress Risk
SCD (especially Type 2) only becomes useful in stress-reporting if you care about point‑in‑time views of portfolios or 
risk hierarchies that differ from “today’s latest view.”

## as Of Today
- for daily runs SCD adds complexity without giving you anything
- Our daily runs are pure as‑of‑today EOD run using today's dimension's (eg:  org (LOB), LE  hierarchy, and data‑source mapping?)

## replay
- historical time series, but with moving boundaries (entities move between LOBs, get merged, etc.).
- You need historical dimension versions to aggregate consistently “as defined then,” which is exactly the SCD Type‑2 
  use‑case.
- Typical stress pipeline pieces where SCD (Type 2) is justified:

Legal entity hierarchy changes + Reg and management reports sometimes need:

“As‑of‑today hierarchy on historical trades” vs

“As‑of‑then hierarchy for historical runs.”

**SCD lets you support both styles if you design facts with an “as‑of date” join to dimension versions.**

### UC1: LOB / Business line allocation
Positions move between desks/LOBs, or the tree is reorganized.
SCD enables: “Show 2019–2025 losses by LOB as they were at the time” for governance, model validation, or internal audit.

Data source / feed lineage

### UC2: For model risk or audit, you might need: “Which upstream system fed exposure X when we ran the 2024 CCAR submission?”

Capturing data‑source as a Type‑2 dimension gives an audit trail for “where did this number come from” style questions.

These are usually driven by regulators, internal model‑risk, or audit needs (e.g., prove what you knew and how you 
aggregated it at a specific date), not by day‑to‑day risk management.

## When you can safely avoid SCD
You can generally stick to Type‑1 (or even just current reference tables) if:
Your users only ever ask “what is today’s stress” and do not challenge historical numbers with “but the hierarchy changed.”
- Historical reruns are re‑runs with current setup, not replays of the originally‑filed numbers.
- Your audit/regulators are satisfied with “we recompute with current hierarchies” rather than “we can reproduce what 
  we reported then.”
- A useful rule of thumb: if no one is asking for regenerating past reports exactly as produced, or for trend 
  analysis under historical org definitions, then SCD for LOB/LE/data‑source will mostly be theoretical overhead.

## Practical design for your situation
* Start with Type‑1 for LOB, LE, data‑source.
* Add a very targeted Type‑2 only where you already have explicit requirements (e.g., LE hierarchy as‑of reporting date for regulatory submissions).
* Keep fact tables date‑keyed so you can join to dimension versions later if requirements harden (i.e., make it easy to retrofit SCD).
* That way, your daily EOD stress reports stay simple, and you only pay the SCD cost where there is a concrete point‑in‑time reporting or audit requirement.

## Implementation Example:

- Type 2 SCD tracks historical changes to attributes like LOB codes or LE hierarchies by creating new dimension rows for 
each change, preserving prior versions via effective dates.

```sql
Dimension Table (LOB and LE tables with these core columns):
Natural key: lob_code or le_id (unique business identifier).
Surrogate key: lob_sk or le_sk (INT, auto-increment for joins).
Version attributes: Type 2 columns to manage history.

Column	                Data Type	    Purpose
lob_name	            VARCHAR	        Overwrite on change (Type 1 behavior if stable).
parent_lob_code	        VARCHAR	        Hierarchy parent; changes trigger new row.
effective_start_date	DATE	        Start of this version's validity.
effective_end_date	    DATE	        End of validity (9999-12-31 for current).
is_current	            CHAR(1)	        'Y'/'N' flag for active row.

Same structure for LE (add le_name, legal_status, region_code etc.). 
```

- ***PIT queries:*** Index on natural key + effective dates
### ETL Implementation Steps 

- Process daily source changes (e.g., from staging) via MERGE or staged INSERT/UPDATE:

1) Detect changes: LEFT JOIN source to current target rows (WHERE is_current = 'Y'). Flag as:
* NEW: No match on natural key.
* CHANGED: Match exists but attributes differ (e.g., lob_name <> source.lob_name OR parent_lob_code <> source.
  parent_lob_code).
* UNCHANGED: Match and identical.

2) Expire old versions (for CHANGED records):
```sql 
UPDATE dim_lob
SET is_current = 'N',
effective_end_date = CURRENT_DATE - INTERVAL 1 DAY
WHERE lob_code = ? AND is_current = 'Y';
```

3) Insert new versions (NEW + CHANGED):
```sql
INSERT INTO dim_lob (lob_sk, lob_code, lob_name, parent_lob_code, effective_start_date, effective_end_date, is_current)
VALUES (NEXTVAL('lob_sk_seq'), source.lob_code, source.lob_name, source.parent_lob_code, CURRENT_DATE, '9999-12-31', 
'Y');

```
Handle hierarchies recursively if LE/LOB trees change deeply (use recursive CTE to propagate).

### Fact Table Joins for Stress Reports.In risk fact tables (e.g., stress positions), store surrogate key + as-of date:

```sql
fact_stress (
report_date DATE,
lob_sk INT,           -- References historical LOB version
le_sk INT,
exposure DECIMAL
)
```
Query point-in-time (PIT) LOB/LE for a report date:

```sql
SELECT f.exposure, l.lob_name, le.le_name
FROM fact_stress f
JOIN dim_lob l ON f.lob_sk = l.lob_sk
AND f.report_date >= l.effective_start_date
AND f.report_date < l.effective_end_date
JOIN dim_le le ON f.le_sk = le.le_sk
AND f.report_date >= le.effective_start_date
AND f.report_date < le.effective_end_date
WHERE f.report_date = '2025-09-30';
```

This reconstructs "LOB/LE as of report date" for EOD replays or historical trends. For daily EOD, 
join only current (is_current='Y').