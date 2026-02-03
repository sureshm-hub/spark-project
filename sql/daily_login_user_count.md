From table user_login (user_id, login_ts) Daily login count trend for past 15 days.

select 
    count(DISTINCT user_id) as daily_users,
    cast(login_ts AS DATE) as logon_date 
from 
    user_login
where 
    # current_timestamp - login_ts <= 15 
    # clause might not work as expected in all SQL dialects PostgreSQL may require explicit interval syntax, like INTERVAL '15 days'
login_ts >= CURRENT_DATE - INTERVAL '15' DAY
group by
    cast(login_ts as date)
order by 
    logon_date