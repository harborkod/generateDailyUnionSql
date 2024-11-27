from datetime import date, timedelta


def generate_daily_union_sql(start_date, end_date, table_name, columns, where_conditions=None, group_by=None):
    """
    生成每天的 UNION ALL SQL 查询。

    参数:
    - start_date: 起始日期 (date 对象)
    - end_date: 结束日期 (date 对象)
    - table_name: 查询的表名 (字符串)
    - columns: 查询的列 (列表)
    - where_conditions: 固定的 WHERE 条件 (字典，可选)
    - group_by: GROUP BY 的列 (列表，可选)

    返回:
    - 完整的 SQL 查询字符串
    """
    sql_statements = []
    current_date = start_date
    while current_date <= end_date:
        # 拼接动态的 WHERE 条件
        where_clause = ""
        if where_conditions:
            daily_conditions = where_conditions.copy()
            daily_conditions['create_date'] = f"'{current_date}'"  # 添加日期条件
            where_clause = " WHERE " + " AND ".join([f"{key} = {value}" for key, value in daily_conditions.items()])
        else:
            where_clause = f" WHERE create_date = '{current_date}'"  # 只有日期条件

        # 拼接 SELECT 列和 GROUP BY
        select_columns = ", ".join(columns)
        group_by_clause = f" GROUP BY {', '.join(group_by)}" if group_by else ""

        # 生成单日查询
        sql = f"""
SELECT {select_columns}
FROM {table_name}{where_clause}{group_by_clause}
"""
        sql_statements.append(sql.strip())
        current_date += timedelta(days=1)

    # 合并所有查询
    return "\nUNION ALL\n".join(sql_statements)


# 示例参数
start_date = date(2024, 5, 1)
end_date = date(2024, 8, 31)
table_name = "settlement.settlement"
columns = ["create_date", "node_id", "status", "SUM(money) AS money"]
where_conditions = {
    "customer_code": "'NZB125816'",
    "settlement.reason": "13",
    "node_id": "'7122590'"
}
group_by = ["node_id", "status"]

# 生成 SQL
final_sql = generate_daily_union_sql(start_date, end_date, table_name, columns, where_conditions, group_by)
print(final_sql)
