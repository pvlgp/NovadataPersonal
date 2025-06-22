create table user_events (
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
engine = MergeTree()
order by (event_time, user_id)
ttl event_time + interval 30 day;

create table user_events_agg(
    event_date Date,
    event_type String,
    unique_users AggregateFunction(uniq, UInt32),
    total_spent AggregateFunction(sum, UInt32),
    total_actions AggregateFunction(count, UInt32)
)
engine = AggregatingMergeTree()
order by (event_date, event_type)
ttl event_date + interval 180 day;

create materialized view mv_user_events
to user_events_agg
as
select
    toDate(event_time) as event_date,
    event_type,
    uniqState(user_id) as unique_users,
    sumState(points_spent) as total_spent,
    countState() as total_actions
from user_events
group by event_date, event_type

INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

select
    uea.event_date,
    uea.event_type,
    uniqMerge(uea.unique_users) as unique_users,
    sumMerge(uea.total_spent) as total_spent,
    countMerge(uea.total_actions) as total_actions
from user_events_agg uea
group by uea.event_date, uea.event_type
order by uea.event_date;

WITH day_0_users AS (
    SELECT user_id
    FROM user_events
    WHERE event_time >= now() - interval 7 day AND event_time < now() - interval 7 day -- начальный период
),
returned_users AS (
    SELECT user_id, COUNT(DISTINCT event_date) AS return_days
    FROM (
        SELECT user_id, toDate(event_time) AS event_date
        FROM user_events
        WHERE event_time >= now() - interval 6 day AND event_time < now() -- последующие 7 дней
    ) AS ru
    GROUP BY user_id
)
SELECT
    (SELECT COUNT(DISTINCT user_id) FROM day_0_users) AS total_users_day_0,
    COUNT(DISTINCT returned_users.user_id) AS returned_in_7_days,
    (COUNT(DISTINCT returned_users.user_id) / (SELECT COUNT(DISTINCT user_id) FROM day_0_users)) * 100 AS retention_7d_percent
FROM returned_users
JOIN day_0_users ON returned_users.user_id = day_0_users.user_id;

