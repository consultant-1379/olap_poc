INSERT
INTO
    narrow_table_day_agg
    (
        tablename,
        column_string1_col__1_1,
        column_string2_col__1_1,
        column_string3_col__1_1,
        column_string4_col__1_1,
        column_string5_col__1_1,
        column_string6_col__1_1,
        column_string7_col__1_1,
        column_string8_col__1_1,
        column_int1_col__4_1,
        column_int1_col__4_2,
        column_int1_col__4_3,
        column_int1_col__4_4,
        column_int2_col__2_1,
        column_int2_col__2_2,
        column_int3_col__1_1,
        column_int4_col__1_1,
        column_long1_col__4_1,
        column_long1_col__4_2,
        column_long1_col__4_3,
        column_long1_col__4_4,
        column_long2_col__2_1,
        column_long2_col__2_2,
        column_long3_col__1_1,
        column_long4_col__1_1,
        column_string9_col__1_1,
        column_string10_col__1_1,
        column_string11_col__1_1,
        column_string12_col__1_1,
        column_string13_col__1_1,
        column_string14_col__1_1,
        column_string15_col__1_1,
        column_string16_col__1_1,
        column_int5_col__4_1,
        column_int5_col__4_2,
        column_int5_col__4_3,
        column_int5_col__4_4,
        column_int6_col__1_1,
        column_int7_col__1_1,
        column_int8_col__2_1,
        column_int8_col__2_2,
        column_long5_col__4_1,
        column_long5_col__4_2,
        column_long5_col__4_3,
        column_long5_col__4_4,
        column_long6_col__1_1,
        column_long7_col__1_1,
        column_long8_col__2_1,
        column_long8_col__2_2,
        column_datetime1_col__1_1,
        column_datetime2_col__1_1,
        column_datetime3_col__1_1
    )
SELECT
    '@tablename',
    MAX(column_string1_col__1_1),
    MAX(column_string2_col__1_1),
    MAX(column_string3_col__1_1),
    MAX(column_string4_col__1_1),
    MAX(column_string5_col__1_1),
    MAX(column_string6_col__1_1),
    MAX(column_string7_col__1_1),
    MAX(column_string8_col__1_1),
    SUM(column_int1_col__4_1),
    SUM(column_int1_col__4_2),
    SUM(column_int1_col__4_3),
    SUM(column_int1_col__4_4),
    SUM(column_int2_col__2_1),
    SUM(column_int2_col__2_2),
    SUM(column_int3_col__1_1),
    SUM(column_int4_col__1_1),
    SUM(column_long1_col__4_1),
    SUM(column_long1_col__4_2),
    SUM(column_long1_col__4_3),
    SUM(column_long1_col__4_4),
    SUM(column_long2_col__2_1),
    SUM(column_long2_col__2_2),
    SUM(column_long3_col__1_1),
    SUM(column_long4_col__1_1),
    MAX(column_string9_col__1_1),
    MAX(column_string10_col__1_1),
    MAX(column_string11_col__1_1),
    MAX(column_string12_col__1_1),
    MAX(column_string13_col__1_1),
    MAX(column_string14_col__1_1),
    MAX(column_string15_col__1_1),
    MAX(column_string16_col__1_1),
    SUM(column_int5_col__4_1),
    SUM(column_int5_col__4_2),
    SUM(column_int5_col__4_3),
    SUM(column_int5_col__4_4),
    SUM(column_int6_col__1_1),
    SUM(column_int7_col__1_1),
    SUM(column_int8_col__2_1),
    SUM(column_int8_col__2_2),
    SUM(column_long5_col__4_1),
    SUM(column_long5_col__4_2),
    SUM(column_long5_col__4_3),
    SUM(column_long5_col__4_4),
    SUM(column_long6_col__1_1),
    SUM(column_long7_col__1_1),
    SUM(column_long8_col__2_1),
    SUM(column_long8_col__2_2),
    MAX(column_datetime1_col__1_1),
    MAX(column_datetime2_col__1_1),
    MAX(column_datetime3_col__1_1)
FROM
    @tablename
WHERE
    column_datetime3_col__1_1 BETWEEN DATETIME '@date_id1' AND DATETIME '@date_id2'
GROUP BY
        HOUR(column_datetime3_col__1_1);