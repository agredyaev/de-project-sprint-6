DROP TABLE IF EXISTS NEYBYARU__DWH.l_user_group_activity CASCADE;

CREATE TABLE NEYBYARU__DWH.l_user_group_activity (
    hk_l_user_group_activity bigint primary key,
    hk_user_id bigint NOT NULL CONSTRAINT fk_l_user_message_user REFERENCES NEYBYARU__DWH.h_users (hk_user_id),
    hk_group_id bigint NOT NULL CONSTRAINT fk_l_admin_groups REFERENCES NEYBYARU__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src VARCHAR(20)
)
ORDER BY
    load_dt SEGMENTED BY hk_user_id ALL nodes PARTITION BY load_dt :: DATE
GROUP BY
    calendar_hierarchy_day(load_dt :: DATE, 3, 2);