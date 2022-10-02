DROP TABLE IF EXISTS NEYBYARU__STAGING.group_log;

CREATE TABLE NEYBYARU__STAGING.group_log (
    group_id INT PRIMARY KEY,
    user_id INT,
    user_id_from INT,
    event VARCHAR(10),
    datetime TIMESTAMP(6)
)
ORDER BY
    group_id SEGMENTED BY HASH(group_id) ALL nodes PARTITION BY datetime :: DATE
GROUP BY
    calendar_hierarchy_day(datetime :: DATE, 3, 2);