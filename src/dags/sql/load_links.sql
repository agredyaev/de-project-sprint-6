TRUNCATE TABLE NEYBYARU__DWH.l_user_group_activity;

INSERT INTO
    NEYBYARU__DWH.l_user_group_activity(
        hk_l_user_group_activity,
        hk_user_id,
        hk_group_id,
        load_dt,
        load_src
    )
SELECT
    DISTINCT HASH(hu.hk_user_id, hg.hk_group_id),
    hu.hk_user_id,
    hg.hk_group_id,
    now() AS load_dt,
    's3' AS load_src
FROM
    NEYBYARU__STAGING.group_log AS g
    LEFT JOIN NEYBYARU__DWH.h_users AS hu ON g.user_id = hu.user_id
    LEFT JOIN NEYBYARU__DWH.h_groups AS hg ON g.group_id = hg.group_id
WHERE
    HASH(hu.hk_user_id, hg.hk_group_id) NOT IN (
        SELECT
            hk_l_user_group_activity
        FROM
            NEYBYARU__DWH.l_user_group_activity
    );

