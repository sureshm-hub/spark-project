From Snowflake table stress_data (scenario_id, position_id, value, timestamp),
compute running total per scenario with latest 30 days data only.

```sql
select scenario_id, timestamp,
sum() OVER(
        PARTITION BY SCENARIO_ID ORDER BY TIMESTAMP
         ROWS BETWEEN UNBOUNDED  PRECEDING AND CURRENT ROW
        ) AS running_total
from stress_data
where timestamp >= current_date - 30
```

-- Candidate should mention these optimizations:
ALTER TABLE stress_data CLUSTER BY (scenario_id, timestamp);

-- Matches exact filter/order pattern → micro-partitions pruned perfectly

ALTER WAREHOUSE compute_wh
SET WAREHOUSE_SIZE = 'LARGE'
AUTO_SUSPEND = 60;  -- Cost control from resume


-- Strong candidate explains:
- NULL values in `value` → SUM skips them automatically
- Multiple timestamps same day → ORDER BY stable, no gaps
- Gaps in data → running total carries forward correctly
- Warehouse sizing → scans 2-3TB efficiently with clustering