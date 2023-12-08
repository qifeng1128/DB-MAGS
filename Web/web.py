import dash
from dash import html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from dash import dcc
import paramiko
import os
from stat import S_ISDIR as isdir
import json
import re
import dash
import flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple
import copy


# 异常类型
anomaly_dict = dict()
anomaly_dict["lock"] = 0
anomaly_dict["flow"] = 1
anomaly_dict["resource"] = 2
anomaly_dict["slow"] = 3
anomaly_dict["dump"] = 4

tempt_dict1 = dict()
tempt_dict1['lock--->slow'] = 'Lock Conflict(Table Lock)--->Slow SQL'
tempt_dict1['flow--->lock'] = 'Traffic Surge (Overall Workload)--->Lock Conflict (Row Lock)'
tempt_dict1['flow--->resource'] = 'Traffic Surge (Overall Workload)--->Resource Bottleneck'
tempt_dict1['resource--->slow'] = 'Resource Bottleneck (CPU)--->Slow SQL'
tempt_dict1['dump--->lock'] = 'Data Table Backup--->Lock Conflict (Row Lock)'

tempt_dict2 = dict()
tempt_dict2['lock'] = 'Lock Conflict'
tempt_dict2['flow'] = 'Traffic Surge'
tempt_dict2['resource'] = 'Resource Bottlenect'
tempt_dict2['slow'] = 'Slow SQL'
tempt_dict2['dump'] = 'Data Table Backup'

anomaly_count = len(anomaly_dict)

# 两种异常之间的因果关系表
cause_and_effect = [[0] * anomaly_count for _ in range(anomaly_count)]
cause_and_effect[anomaly_dict["lock"]][anomaly_dict["slow"]] = 1   # 锁冲突 ---> 慢sql
cause_and_effect[anomaly_dict["flow"]][anomaly_dict["lock"]] = 1   # 流量突增 ---> 锁冲突
cause_and_effect[anomaly_dict["flow"]][anomaly_dict["resource"]] = 1   # 流量突增 ---> 资源瓶颈
cause_and_effect[anomaly_dict["resource"]][anomaly_dict["slow"]] = 1   # 资源瓶颈 ---> 慢sql
cause_and_effect[anomaly_dict["dump"]][anomaly_dict["lock"]] = 1   # 数据表备份 ---> 锁冲突

tempt_dict = dict()
tempt_dict['Table Lock'] = 'table_lock'
tempt_dict['Metadata Lock'] = 'meta_data_lock'
tempt_dict['Record Lock'] = 'record_lock'
tempt_dict['Single SQL'] = 'flow_sql'
tempt_dict['Missing Index'] = 'missing_index'
tempt_dict['Excessive Index'] = 'too_much_index'
tempt_dict['Implicit Conversion'] = 'implicit_conversion'
tempt_dict['Multi-table Join'] = 'query_with_too_much_join'
tempt_dict['Order By'] = 'order_by'
tempt_dict['Group By'] = 'group_by'
tempt_dict['Large Table Scan'] = 'query_whole_table'
tempt_dict['I/O'] = 'io'
tempt_dict['Disk'] = 'disk'
tempt_dict['Memory'] = 'mem'
tempt_dict['Network'] = 'net'

# 输入故障类型，输入多种注入方式
def inject_sql(inject_anomaly):
    result = []
    if len(inject_anomaly) == 1:
        one_result = str(inject_anomaly[0]) + "+"
        result.append(one_result)
    elif len(inject_anomaly) == 2:
        if cause_and_effect[anomaly_dict[inject_anomaly[0]]][anomaly_dict[inject_anomaly[1]]] == 1:
            one_result = str(inject_anomaly[0]) + "--->" + str(inject_anomaly[1]) + "+"
            result.append(one_result)
        elif cause_and_effect[anomaly_dict[inject_anomaly[1]]][anomaly_dict[inject_anomaly[0]]] == 1:
            one_result = str(inject_anomaly[1]) + "--->" + str(inject_anomaly[0]) + "+"
            result.append(one_result)
    else:
        for one_anomaly in inject_anomaly:
            tempt = copy.deepcopy(inject_anomaly)
            tempt.remove(one_anomaly)
            # 寻找是否存在因果关系
            for cause in tempt:
                if cause_and_effect[anomaly_dict[cause]][anomaly_dict[one_anomaly]] == 1:
                    tempt1 = copy.deepcopy(tempt)
                    tempt1.remove(cause)
                    result_before = inject_sql(tempt1)
                    for one_inject_sql in result_before:
                        tem = one_inject_sql + str(cause) + "--->" + str(one_anomaly) + "+"
                        result.append(tem)
    # 加入伴随关系
    tem = ""
    for one_anomaly in inject_anomaly:
        tem = tem + str(one_anomaly) + "+"
    result.append(tem)
    return result

# 对不同注入方式进行去重
# 根据是否同时存在两个异常的因果和伴随关系，对注入方式进行增加
def remove_duplicate(anomaly_list, relation_bool):
    result = inject_sql(anomaly_list)
    total = []
    for i in result:
        re = i.split("+")
        tempt = []     # 将组合的注入方式进行分解
        flag = 0       # 判断注入方式中是否存在因果关系
        for j in re:
            if j != "":
                tempt.append(j)
            if "--->" in j:
                flag = 1
        tempt.sort()   # 排序用于去重
        if tempt not in total:
            # 同时存在因果和伴随，增加注入方式
            if relation_bool == True and flag == 1:
                for cause_effect in tempt:
                    if "--->" in cause_effect:
                        cause = cause_effect.split("--->")[0]
                        effect = cause_effect.split("--->")[1]
                        # 再次注入因
                        tempt1 = copy.deepcopy(tempt)
                        tempt1.append(cause)
                        total.append(tempt1)
                        # 再次注入果
                        tempt2 = copy.deepcopy(tempt)
                        tempt2.append(effect)
                        total.append(tempt2)
                        # 再次注入因和果
                        tempt3 = copy.deepcopy(tempt)
                        tempt3.append(cause)
                        tempt3.append(effect)
                        total.append(tempt3)
            total.append(tempt)
    return total

# 将故障注入按格式进行组合（优先因果关系再伴随关系，伴随关系中flow_sql需优先便于修改故障注入的参数）
# 将单独的flow_sql、flow_sql+resource进行排除，因为没有sql注入仅有flow_sql并不成立
def modify_multi_fault(anomaly_list, relation_bool):
    result = []
    fault_list = remove_duplicate(anomaly_list, relation_bool)
    for fault_sql in fault_list:   # 某种排列组合方式
        # 对因果和伴随进行分离
        cause_effect = []
        fault_else = []
        for one_fault in fault_sql:
            if "--->" in one_fault:
                cause_effect.append(one_fault)
            else:
                fault_else.append(one_fault)
        if len(fault_else) == 1 and fault_else[0] == "flow":   # 单独的flow_sql
            continue
        elif len(fault_else) == 2 and "flow" in fault_else and "resource" in fault_else:  # flow_sql + resource
            continue
        else:
            tempt = ""
            for i in cause_effect:     # 因果关系优先
                tempt = tempt + i + "+"
            if "flow" in fault_else:   # flow优先
                tempt = tempt + "flow+"
            for i in fault_else:
                if i != "flow":
                    tempt = tempt + i + "+"
            result.append(tempt[0:-1])
    return result

app1 = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

app1.layout = html.Div(
    dbc.Container(
        [
            html.H1('Database Performance Anomaly Injection and Metric Collection:'),
            html.Br(),

            html.P('Users can use this system to inject custom database performance anomaly scenarios, including single anomalies and multiple anomalies, and collect the corresponding monitoring metric data.', style={'font-size': '20px'}),

            html.Br(),

            html.P('1. Single anomaly or Multiple anomaly:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.RadioItems(
                id='anomaly',
                inline=True,
                options=[
                    {'label': 'single anomaly', 'value': 'single'},
                    {'label': 'multiple anomaly', 'value': 'compound'}
                ]
            ),
            html.Br(),

            ### 隐藏（异常大类分别为单选和多选）
            html.Div(id='hide_anomaly_category'),


            ### 隐藏（异常小类，根据大类选择的不同进行展示）
            html.Div(id='hide_anomaly_type'),

            ### 隐藏（多异常的小类）
            html.Div(id='hide_compound_anomaly_type'),


            html.P('5. The Duration of Background Workload:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Input(id='duration',
                      placeholder='number',
                      type='number',
                      min=0,
                      max=300,
                      step=1,
                      style={'width': '300px'}),
            html.P(id='output-number', style={'font-size': '20px'}),
            html.Br(),

            ### 隐藏（因异常注入时间）
            html.Div(id='hide_cause_inject_time'),


            html.P('6. Time to Inject the Effect Anomaly:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Input(id='effect_inject_time',
                      placeholder='number',
                      type='number',
                      min=0,
                      max=300,
                      step=1,
                      style={'width': '300px'}),
            html.Br(),

            html.P('7. Sleep Time After Each Transaction:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Input(id='sleep_time',
                      placeholder='number',
                      type='number',
                      min=0,
                      max=1,
                      step=0.001,
                      style={'width': '300px'}),
            html.Br(),

            ### 隐藏（因异常注入数量）
            html.Div(id='hide_cause_inject_number'),


            html.P('8. Inject Number of the Effect Anomaly:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Input(id='effect_inject_number',
                      placeholder='number',
                      type='number',
                      min=0,
                      max=30,
                      step=1,
                      style={'width': '300px'}),
            html.Br(),

            html.P('9. Number of the dataset:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Input(id='dataset_number',
                      placeholder='number',
                      type='number',
                      min=0,
                      max=200,
                      step=1,
                      style={'width': '300px'}),
            html.Br(),

            html.P('10. Path To Store Dataset(eg: D:/mysqlrc/dataset/)', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Input(id='path',
                      placeholder='text',
                      type='text',
                      maxLength=200,
                      style={'width': '300px'}),
            html.Br(),

            dbc.Button(
                'Database Anomaly Injection and Metric Collection',
                id='submit'
            ),

            html.Br(),

            html.Br(),

            html.Div(id='redirect-url-container'),
            dbc.Button('View Metric Changes', id='submit1', style={'marginRight': '10px'}),

            html.P(id='feedback'),

            ### 隐藏（多异常的小类）
            html.Div(id='hide_jindutiao'),


        ],
        style={
            'margin-top': '50px',
            'margin-bottom': '200px',
        }
    )
)

@app1.callback(
    Output('redirect-url-container', 'children'),
    [Input('submit1', 'n_clicks')],
)
def jump_to_target(n_clicks):
    ctx = dash.callback_context

    if ctx.triggered[0]['prop_id'] == 'submit1.n_clicks':
        return html.A("Link to Grafana", href='http://113.31.103.14:3000/d/mysql/mysql_new?orgId=1&from=now-5m&to=now&refresh=10s', target="_blank")


    return dash.no_update


@app1.callback(
    Output(component_id='hide_jindutiao', component_property='children'),
    Input('submit', 'n_clicks'),
    prevent_initial_call=True
)
def show_progess(n_click):
    return [
               dcc.Interval(id="progress_afrunk", interval=1000, n_intervals=0),
               dbc.Progress(id="progress", value=75),
               html.P(id="progress-label"),  # 用于显示文本标签的段落
    ]

# afrunk
@app1.callback(
    [Output("progress", "value"),  Output("progress-label", "children")],
    [Input("progress_afrunk", "n_intervals"), Input("duration", "value"), Input("dataset_number", "value")],
)
def update_progress(n, duration, dataset_number):
    # 异常注入时间：duration
    # 指标采集时间：20秒左右
    # 拷贝文件时间：60秒左右
    total_time = int(dataset_number) * (int(duration) + 20 + 60)
    n = 1.0 * n / total_time * 100         # 共执行200秒
    if n is None:
        return [0]
    progress = min(n % 110, 100)
    label = f"{progress} %"  # 更新文本标签
    return progress, label

single_anomaly_dict = dict()
single_anomaly_dict["表锁"] = "1"
single_anomaly_dict["元数据锁"] = "2"
single_anomaly_dict["行锁"] = "3"
single_anomaly_dict["索引缺失"] = "4"
single_anomaly_dict["索引过多"] = "5"
single_anomaly_dict["隐式转换"] = "6"
single_anomaly_dict["join多表"] = "7"
single_anomaly_dict["order by"] = "8"
single_anomaly_dict["group by"] = "9"
single_anomaly_dict["大表扫描"] = "10"
single_anomaly_dict["数据表备份"] = "11"
single_anomaly_dict["CPU"] = "12"
single_anomaly_dict["I/O"] = "13"
single_anomaly_dict["磁盘"] = "14"
single_anomaly_dict["内存"] = "15"
single_anomaly_dict["网络"] = "16"

def get_command(host_name, port, user_name, password, command):
    sshClient = paramiko.SSHClient()
    sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sshClient.connect(hostname=host_name, port=port, username=user_name, password=password)
    # sshClient.connect(hostname="113.31.103.14", port=22, username="root", password="Meituan312")

    shell = sshClient.invoke_shell()

    command = command + "\n"

    shell.sendall(command)

    path_root = ""

    while True:
        data = shell.recv(2048).decode()
        print(data)
        if "path" in data:
            path_root = data.split(":")[1]
            continue
        if "over" in data:  # 执行结束
            break
    sshClient.close()

    return path_root

def down_from_remote(host_name, port, user_name, password, remote_dir_name, local_dir_name):
    # 连接远程服务器
    t = paramiko.Transport((host_name, port))
    t.connect(username=user_name, password=password)
    sftp = paramiko.SFTPClient.from_transport(t)

    """远程下载文件"""
    remote_file = sftp.stat(remote_dir_name)
    if isdir(remote_file.st_mode):
        # 文件夹，不能直接下载，需要继续循环
        check_local_dir(local_dir_name)

        tempt = remote_dir_name.split("/")[-1]
        root_path = os.path.join(local_dir_name, tempt)
        # 创建文件夹
        if not os.path.exists(root_path):
            os.makedirs(root_path)

        print('开始下载文件夹：' + remote_dir_name)
        for remote_file_name in sftp.listdir(remote_dir_name):
            sub_remote = os.path.join(remote_dir_name, remote_file_name)
            sub_remote = sub_remote.replace('\\', '/')
            sub_local = os.path.join(root_path, remote_file_name)
            sub_local = sub_local.replace('\\', '/')
            down_from_remote(host_name, port, user_name, password, sub_remote, sub_local)
    else:
        # 文件，直接下载
        print('开始下载文件：' + remote_dir_name)
        sftp.get(remote_dir_name, local_dir_name)

    t.close()
    print("----over------")


def check_local_dir(local_dir_name):
    """本地文件夹是否存在，不存在则创建"""
    if not os.path.exists(local_dir_name):
        os.makedirs(local_dir_name)


@app1.callback(
    Output('feedback', 'children'),
    Input('submit', 'n_clicks'),
    [
        State('anomaly', 'value'),      # 单一异常 or 多异常
        State('anomaly_category', 'value'),    # 异常大类
        State('anomaly_type', 'value'),        # 异常小类
        State('compound_anomaly_type', 'value'),   # 多异常的小类
        State('duration', 'value'),     # 背景工作负载运行时间
        State('cause_inject_time', 'value'),   # 因异常注入时间
        State('effect_inject_time', 'value'),  # 果异常注入时间
        State('sleep_time', 'value'),   # 休眠时间
        State('cause_inject_number', 'value'),    # 因异常注入数量
        State('effect_inject_number', 'value'),   # 果异常注入数量
        State('dataset_number', 'value'),     # 数据集数量
        State('path', 'value'),     # 文件存储路径
    ],
    prevent_initial_call=True
)


def fetch_info(n_clicks, anomaly, anomaly_category, anomaly_type, compound_anomaly_type, duration, cause_inject_time, effect_inject_time,
               sleep_time, cause_inject_number, effect_inject_number, dataset_number, path):
    if anomaly == 'single':
        # 单一异常
        command = "python3 /root/mysqlrc/fault_injection/Case_make/Case_make.py"
        new_anomaly_type = single_anomaly_dict[anomaly_type]
        command = command + " -d " + str(duration) + " -t " + str(effect_inject_time) + " -x " + str(30) + " -i " + \
                  new_anomaly_type + " -s " + str(sleep_time) + " -c " + str(1) + " -e " + \
                  str(effect_inject_number) + " -n " + str(1)
    else:
        # 多异常
        command = "python3 /root/mysqlrc/fault_injection/Case_make/Case_make1.py"
        new_anomaly_ty = ""
        new_d1 = {v: k for k, v in tempt_dict1.items()}
        new_d2 = {v: k for k, v in tempt_dict2.items()}
        tt = anomaly_type.split("+")
        for t in tt:
            if "--->" in t:
                new_anomaly_ty = new_anomaly_ty + new_d1[t] + "+"
            else:
                new_anomaly_ty = new_anomaly_ty + new_d2[t] + "+"
        new_anomaly_type = ""
        for anomaly in compound_anomaly_type:
            if anomaly != "none":
                new_anomaly_type = new_anomaly_type + tempt_dict[anomaly] + "+"
        command = command + " -d " + str(duration) + " -t " + str(effect_inject_time) + " -x " + str(cause_inject_time) + " -i '" + \
                  new_anomaly_ty[:-1] + "' -k '" + new_anomaly_type[:-1] + "' -s " + str(sleep_time) + " -c " + str(cause_inject_number) + " -e " + \
                  str(effect_inject_number) + " -n " + str(1)

    host_name = "113.31.103.14"
    port = 22
    user_name = "root"
    password = "Meituan312"
    for i in range(int(dataset_number)):
        # 执行故障注入
        one_path = get_command(host_name, port, user_name, password, command)
        one_new_path = one_path.replace(" ", "").replace("\r", "").replace("\n", "")
        # 拷贝服务器上数据集
        down_from_remote(host_name, port, user_name, password, one_new_path, path)


    # file = open("tempt.txt", "w")
    # file.write(command)
    # file.close()
    # return '提交成功！'


# 异常大类（选择单一异常或多异常）
@app1.callback(
    Output(component_id='hide_anomaly_category', component_property='children'),
    [Input(component_id='anomaly', component_property='value')]
)
def show_hide_anomaly_category(value):
    if value == 'single':
        return [
            html.P('2. Anomaly Category:', style={'font-size': '20px'}),
            html.Hr(),
            dcc.Dropdown(
                id='anomaly_category',
                placeholder='Single Choice',
                options=[
                    {'label': 'Lock Conflict', 'value': '锁冲突'},
                    {'label': 'Traffic Surge', 'value': '流量突增'},
                    {'label': 'Slow SQL', 'value': '慢查询'},
                    {'label': 'Resource Bottlenect', 'value': '资源瓶颈'},
                    {'label': 'Data Table Backup', 'value': '数据表备份'},
                ],
                style={
                    'width': '300px'
                }
            ),
            html.Br(),
        ]
    else:
        return [
            html.P('2. Anomaly Category:', style={'font-size': '20px'}),
            html.Hr(),
            dcc.Dropdown(
                id='anomaly_category',
                placeholder='Multiple Choice',
                multi=True,
                options=[
                    {'label': 'Lock Conflict', 'value': '锁冲突'},
                    {'label': 'Traffic Surge', 'value': '流量突增'},
                    {'label': 'Slow SQL', 'value': '慢查询'},
                    {'label': 'Resource Bottlenect', 'value': '资源瓶颈'},
                    {'label': 'Data Table Backup', 'value': '数据表备份'},
                ],
                style={
                    'width': '300px'
                }
            ),
            html.Br(),
        ]


# 异常小类
@app1.callback(
    Output(component_id='hide_anomaly_type', component_property='children'),
    Input('hide_anomaly_category', 'n_clicks'),
    [
        State('anomaly', 'value'),
        State('anomaly_category', 'value'),
    ],
    prevent_initial_call=True
)
def show_hide_anomaly_category(n_clicks, anomaly, anomaly_category):
    if anomaly == 'single':
        if anomaly_category == '锁冲突':
            return [
                html.P('3. Anomaly Type:', style={'font-size': '20px'}),
                html.Hr(),
                dcc.Dropdown(
                    id='anomaly_type',
                    placeholder='Single Choice',
                    options=[
                        {'label': 'Table Lock', 'value': '表锁'},
                        {'label': 'Metadata Lock', 'value': '元数据锁'},
                        {'label': 'Record Lock', 'value': '行锁'},
                    ],
                    style={
                        'width': '300px'
                    }
                ),
                html.Br(),
            ]
        elif anomaly_category == '流量突增':
            return [
                html.P('3. Anomaly Type:', style={'font-size': '20px'}),
                html.Hr(),
                dcc.Dropdown(
                    id='anomaly_type',
                    placeholder='Single Choice',
                    options=[
                        {'label': 'Single SQL', 'value': '单句sql流量'},         # todo
                        {'label': 'Overall Workload', 'value': '整体工作负载流量'},
                    ],
                    style={
                        'width': '300px'
                    }
                ),
                html.Br(),
            ]
        elif anomaly_category == '慢查询':
            return [
                html.P('3. Anomaly Type:', style={'font-size': '20px'}),
                html.Hr(),
                dcc.Dropdown(
                    id='anomaly_type',
                    placeholder='Single Choice',
                    options=[
                        {'label': 'Missing Index', 'value': '缺失索引'},
                        {'label': 'Excessive Index', 'value': '索引过多'},
                        {'label': 'Implicit Conversion', 'value': '隐式转换'},
                        {'label': 'Multi-table Join', 'value': 'join多表'},
                        {'label': 'Order By', 'value': 'order by'},
                        {'label': 'Group By', 'value': 'group by'},
                        {'label': 'Large Table Scan', 'value': '大表扫描'},
                    ],
                    style={
                        'width': '300px'
                    }
                ),
                html.Br(),
            ]
        elif anomaly_category == '资源瓶颈':
            return [
                html.P('3. Anomaly Type:', style={'font-size': '20px'}),
                html.Hr(),
                dcc.Dropdown(
                    id='anomaly_type',
                    placeholder='Single Choice',
                    options=[
                        {'label': 'CPU', 'value': 'CPU'},
                        {'label': 'I/O', 'value': 'I/O'},
                        {'label': 'Network', 'value': '网络'},
                        {'label': 'Memory', 'value': '内存'},
                        {'label': 'Disk', 'value': '硬盘'},
                    ],
                    style={
                        'width': '300px'
                    }
                ),
                html.Br(),
            ]
        else:
            return [
                html.P('3. Anomaly Type:', style={'font-size': '20px'}),
                html.Hr(),
                dcc.Dropdown(
                    id='anomaly_type',
                    placeholder='Single Choice',
                    options=[
                        {'label': 'Data Table Backup', 'value': '数据表备份'},
                    ],
                    style={
                        'width': '300px'
                    }
                ),
                html.Br(),
            ]
    else:
        if anomaly_category is not None:
            # 获取所有组合方式供用户选择
            new_anomaly_list = []
            for one_anomaly in anomaly_category:
                if one_anomaly == '锁冲突':
                    new_anomaly_list.append("lock")
                elif one_anomaly == '流量突增':
                    new_anomaly_list.append("flow")
                elif one_anomaly == '慢查询':
                    new_anomaly_list.append("slow")
                elif one_anomaly == '资源瓶颈':
                    new_anomaly_list.append("resource")
                else:
                    new_anomaly_list.append("dump")
            tempt = modify_multi_fault(new_anomaly_list, True)
            new_tempt = []
            for one_com_an in tempt:
                new_one = ""
                tempt_list = one_com_an.split("+")
                for t in tempt_list:
                    if "--->" in t:
                        new_one = new_one + tempt_dict1[t] + "+"
                    else:
                        new_one = new_one + tempt_dict2[t] + "+"
                new_tempt.append(new_one[:-1])

            return [
                html.P('3. Anomaly Combinations:', style={'font-size': '20px'}),
                html.Hr(),
                dcc.Dropdown(
                    id='anomaly_type',
                    placeholder='Single Choice',
                    options=[
                        {'label': label, 'value': label}
                        for label in new_tempt
                    ],
                    style={
                        'width': '1000px'
                    }
                ),
                html.Br(),
            ]


# 多异常小类
@app1.callback(
    Output(component_id='hide_compound_anomaly_type', component_property='children'),
    Input('hide_anomaly_type', 'n_clicks'),
    [
        State('anomaly', 'value'),
        State('anomaly_type', 'value'),
    ],
    prevent_initial_call=True
)


def show_hide_anomaly_category(n_clicks, anomaly, anomaly_type):
    if anomaly == 'compound' and anomaly_type is not None:
        label_list = []
        one_list = anomaly_type.split("+")
        for one_anomaly in one_list:
            if one_anomaly == 'Lock Conflict':
                lock_list = ['Table Lock', 'Metadata Lock', 'Record Lock']
                for tmp in lock_list:
                    one_dict = {}
                    one_dict['label'] = tmp
                    one_dict['value'] = tmp
                    label_list.append(one_dict)
            elif one_anomaly == 'Traffic Surge':
                flow_list = ['Single SQL']
                for tmp in flow_list:
                    one_dict = {}
                    one_dict['label'] = tmp
                    one_dict['value'] = tmp
                    label_list.append(one_dict)
            elif one_anomaly == 'Slow SQL':
                slow_list = ['Missing Index', 'Excessive Index', 'Implicit Conversion', 'Multi-table Join', 'Order By', 'Group By', 'Large Table Scan']
                for tmp in slow_list:
                    one_dict = {}
                    one_dict['label'] = tmp
                    one_dict['value'] = tmp
                    label_list.append(one_dict)
            elif one_anomaly == 'Resource Bottlenect':
                resource_list = ['I/O', 'Disk', 'Memory', 'Network']
                for tmp in resource_list:
                    one_dict = {}
                    one_dict['label'] = tmp
                    one_dict['value'] = tmp
                    label_list.append(one_dict)

        one_dict = {}
        one_dict['label'] = 'none'
        one_dict['value'] = 'none'
        label_list.append(one_dict)
        return [
            html.P('4. Anomaly Types:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Checklist(
                id='compound_anomaly_type',
                inline=True,
                options=label_list,
                style={
                    'width': '300px'
                }
            ),
            html.Br(),
        ]
    else:
        return [
            html.P('4. Skip This One (Choose none):', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Checklist(
                id='compound_anomaly_type',
                inline=True,
                options=[{'label':'none', 'value':'none'}],
                style={
                    'width': '300px'
                }
            ),
            html.Br(),
        ]


# 因异常注入时间
@app1.callback(
    Output(component_id='hide_cause_inject_time', component_property='children'),
    [Input(component_id='anomaly', component_property='value')]
)
def show_hide_anomaly_category(value):
    if value == 'compound':
        return [
            html.P('6. Time to Inject the Cause Anomaly:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Input(id='cause_inject_time',
                      placeholder='number',
                      type='number',
                      min=0,
                      max=300,
                      step=1,
                      style={'width': '300px'}),
            html.Br(),
        ]
    else:
        return [
            html.P('6. Skip This One (Choose none):', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Checklist(
                id='cause_inject_time',
                inline=True,
                options=[{'label':'none', 'value':'none'}],
                style={
                    'width': '300px'
                }
            ),
            html.Br(),
        ]


# 因异常注入数量
@app1.callback(
    Output(component_id='hide_cause_inject_number', component_property='children'),
    [Input(component_id='anomaly', component_property='value')]
)
def show_hide_anomaly_category(value):
    if value == 'compound':
        return [
            html.P('8. Inject Number of the Cause Anomaly:', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Input(id='cause_inject_number',
                      placeholder='number',
                      type='number',
                      min=0,
                      max=300,
                      step=1,
                      style={'width': '300px'}),
            html.Br(),
        ]
    else:
        return [
            html.P('8. Skip This One (Choose none):', style={'font-size': '20px'}),
            html.Hr(),
            dbc.Checklist(
                id='cause_inject_number',
                inline=True,
                options=[{'label':'none', 'value':'none'}],
                style={
                    'width': '300px'
                }
            ),
            html.Br(),
        ]

if __name__ == "__main__":
    app1.run_server(debug=True)

