DROP TABLE IF EXISTS NEYBYARU__DWH.s_auth_history;

CREATE TABLE NEYBYARU__DWH.s_auth_history (
    hk_l_user_group_activity bigint NOT NULL CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES NEYBYARU__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from INT,
    event VARCHAR(10),
    event_dt TIMESTAMP(6),
    load_dt datetime,
    load_src VARCHAR(20)
)
ORDER BY
    load_dt SEGMENTED BY user_id_from ALL nodes
PARTITION BY load_dt::DATE
GROUP BY
    calendar_hierarchy_day(load_dt :: DATE, 3, 2);