# Problem
https://leetcode.com/problems/managers-with-at-least-5-direct-reports/description/

# Write your MySQL query statement below
```sql
With managerWith5Employees as (
    select
        manager.id as managerId, manager.name as name, count(1) as directReportCount
    from
        Employee manager inner join  Employee emp on manager.id = emp.managerId
    group by
        manager.id, manager.name
    having
        directReportCount >= 5
) select name from managerWith5Employees
```