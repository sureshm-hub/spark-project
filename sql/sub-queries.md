# duplicate employees

- Subqueries in the FROM Clause (Derived Tables) requires an alias
- Subqueries used for filtering (often called scalar subqueries or list subqueries) do not take an alias.
- Subqueries used to create a single column value (scalar subqueries) do not require a table-level alias, though you 
   will usually want a column alias so the result has a readable name.

 
```sql 
SELECT
    name, rnk
FROM (
    SELECT
        name,
        DENSE_RANK() OVER (PARTITION BY name ORDER BY emp_id) AS rnk
    FROM emp
) 
    AS sub
WHERE rnk = 1;
```