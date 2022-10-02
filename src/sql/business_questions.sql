WITH 
    earliest_groups AS (
        SELECT
            hk_group_id
        FROM
            NEYBYARU__DWH.h_groups hg
        ORDER BY
            registration_dt
        LIMIT
            10
    ), 
    user_group_messages AS (
        SELECT
            luga.hk_group_id,
            COUNT(DISTINCT luga.hk_user_id) cnt_users_in_group_with_messages
        FROM
            NEYBYARU__DWH.l_user_message lum
            JOIN NEYBYARU__DWH.l_user_group_activity luga ON luga.hk_user_id = lum.hk_user_id
        GROUP BY
            luga.hk_group_id
    ),
    user_group_log AS (
        SELECT
            luga.hk_group_id,
            COUNT(DISTINCT luga.hk_user_id) cnt_added_users
        FROM
            NEYBYARU__DWH.l_user_group_activity luga
            INNER JOIN NEYBYARU__DWH.s_auth_history sah ON ISNULL(sah.hk_l_user_group_activity, -1) = ISNULL(luga.hk_l_user_group_activity, -1)
            AND sah.event = 'add'
        GROUP BY
            luga.hk_group_id
    )

SELECT
    eg.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users group_conversion
FROM
    earliest_groups eg
    JOIN user_group_messages ugm ON ugm.hk_group_id = eg.hk_group_id
    JOIN user_group_log ugl ON ugl.hk_group_id = eg.hk_group_id
ORDER BY
    ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC