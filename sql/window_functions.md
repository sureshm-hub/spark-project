Given transactions table (trans_id, branch_id, cust_id, amount, trans_date), find the
second-highest daily total amount per branch for 2025


## Hints
- using DENSE_RANK. Handle ties and NULLs
- Order by is mandatory for Rank(), Dense_Rank(), Row_Number()
- ROW_NUMBER() is preferred for strict deduplicates
- WITH format is known as CTE
- You cannot use a window function like DENSE_RANK() directly in a WHERE clause because window functions are evaluated 
  after the WHERE clause is

```sql
WITH daily_branch_totals_ranked (
    select
        branch_id, trans_date, sum(amount) as daily_total,
        DENSE_RANK()
        OVER (
            PARTITION BY branch_id order by  SUM(amount) desc
        ) as rank
    from
        transactions
    where
        year(trans_date) = 2025
    group by
        branch_id, trans_date
) select branch_id, trans_date, daily_total
from daily_branch_totals_ranked
where  rank = 2
```