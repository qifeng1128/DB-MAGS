
def modify_parameter(cur, global_setting, parameter_name, change_value):
    # 获取参数的原定值
    original_sql = "show session variables like '" + str(parameter_name) + "';"
    #print(original_sql)
    cur.execute(original_sql)
    original_value = int(cur.fetchall()[0][1])
    if global_setting:
        change_sql = "set global " + parameter_name + " = " + str(change_value)
        cur.execute(change_sql)
    else:
        change_sql = "set session " + parameter_name + " = " + str(change_value)
        cur.execute(change_sql)
    return original_value
