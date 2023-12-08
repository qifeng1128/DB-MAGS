# # 方法一
# import pickle
# import numpy as np
# import pandas as pd
# file = open("D:/美团数据库异常检测/DBPA_dataset-main/DBPA_dataset-main/single/fault2_data.pickle",'rb')  # 以二进制读模式（rb）打开pkl文件
# data = pickle.load(file)  # 读取存储的pickle文件
# print(type(data))   # 查看数据类型
# one_data = np.array(data[5][0]).T
# #print(one_data)
# print(one_data.shape)
# pd.DataFrame(one_data).to_csv("missing1.csv")


# import dash
# import dash_html_components as html
# import flask
# from werkzeug.middleware.dispatcher import DispatcherMiddleware
# from werkzeug.serving import run_simple
#
# server = flask.Flask(__name__)
#
#
# @server.route("/")
# def home():
#     return "Hello, Flask!"
#
#
# app1 = dash.Dash(requests_pathname_prefix="/app1/")
# app1.layout = html.Div("Hello, Dash app 1!")
#
# app2 = dash.Dash(requests_pathname_prefix="/app2/")
# app2.layout = html.Div("Hello, Dash app 2!")
#
# application = DispatcherMiddleware(
#     server,
#     {"/app1": app1.server, "/app2": app2.server},
# )
#
# if __name__ == "__main__":
#     run_simple("localhost", 8050, application)

import numpy as np
import pandas as pd

import scipy.stats as stats


def TimeSeriesSimilarityImprove(s1, s2):
    # 取较大的标准差
    sdt = np.std(s1, ddof=1) if np.std(s1, ddof=1) > np.std(s2, ddof=1) else np.std(s2, ddof=1)
    # print("两个序列最大标准差:" + str(sdt))
    l1 = len(s1)
    l2 = len(s2)
    paths = np.full((l1 + 1, l2 + 1), np.inf)  # 全部赋予无穷大
    sub_matrix = np.full((l1, l2), 0)  # 全部赋予0
    max_sub_len = 0
    s1 = (s1 - np.mean(s1)) / np.std(s1)
    s2 = (s2 - np.mean(s2)) / np.std(s2)
    paths[0, 0] = 0
    for i in range(l1):
        for j in range(l2):
            d = s1[i] - s2[j]
            cost = d ** 2
            paths[i + 1, j + 1] = cost + min(paths[i, j + 1], paths[i + 1, j], paths[i, j])
            if np.abs(s1[i] - s2[j]) < sdt:
                if i == 0 or j == 0:
                    sub_matrix[i][j] = 1
                else:
                    sub_matrix[i][j] = sub_matrix[i - 1][j - 1] + 1
                    max_sub_len = sub_matrix[i][j] if sub_matrix[i][j] > max_sub_len else max_sub_len

    paths = np.sqrt(paths)
    s = paths[l1, l2]
    #return s, paths.T, [max_sub_len]

    weight = 0
    for comlen in [max_sub_len]:
        weight = weight + comlen / len(s1) * comlen / len(s2)
    a=1 - weight
    distance=s*a
    return distance

def TimeSeriesSimilarity(s1, s2):
    l1 = len(s1)
    l2 = len(s2)

    s1 = (s1 - np.mean(s1)) / np.std(s1)
    s2 = (s2 - np.mean(s2)) / np.std(s2)
    paths = np.full((l1 + 1, l2 + 1), np.inf)  # 全部赋予无穷大
    paths[0, 0] = 0
    for i in range(l1):
        for j in range(l2):
            d = s1[i] - s2[j]
            cost = d ** 2
            paths[i + 1, j + 1] = cost + min(paths[i, j + 1], paths[i + 1, j], paths[i, j])

    paths = np.sqrt(paths)
    s = paths[l1, l2]
    return s, paths.T


def normalization(data):
    _range = np.max(data) - np.min(data)
    return (data - np.min(data)) / _range


def standardization(data):
    mu = np.mean(data, axis=0)
    sigma = np.std(data, axis=0)
    return (data - mu) / sigma


if __name__ == '__main__':
    # 测试数据
    df1 = pd.read_excel('D:/美团数据库异常检测/mysqlrc/mysqlrc/fault_injection/Benchmark_Dataset/dbpa_dataset_compare/lock.xlsx', index_col=0)
    dbpa_lock_cpu = df1.T['Cpu User'].tolist()
    df2 = pd.read_excel(
        'D:/美团数据库异常检测/mysqlrc/mysqlrc/fault_injection/Benchmark_Dataset/dbpa_dataset_compare/record_lock-monitor.xlsx',
        index_col=0)
    monitor_lock_cpu = df2.T['cpu User'].tolist()[58:58+len(dbpa_lock_cpu)]
    dbpa_lock_cpu = normalization(dbpa_lock_cpu)
    monitor_lock_cpu = normalization(monitor_lock_cpu)
    r, p = stats.pearsonr(np.array(dbpa_lock_cpu), np.array(monitor_lock_cpu))
    print("Cpu_User + lock_single_anomaly:", r)

    # dbpa_con_commit = df1.T['Com Commit'].tolist()
    # monitor_con_commit = df2.T['com commit'].tolist()[58:58 + len(dbpa_con_commit)]
    # dbpa_con_commit = normalization(dbpa_con_commit)
    # monitor_con_commit = normalization(monitor_con_commit)
    # r, p = stats.pearsonr(np.array(dbpa_con_commit), np.array(monitor_con_commit))
    # print("Con_Commit + lock_single_anomaly:", r)

    # dbpa_con_commit = df1.T['Com Commit'].tolist()
    # monitor_lock_cpu = df2.T['com commit'].tolist()[58:58 + len(dbpa_lock_cpu)]
    # dbpa_lock_cpu = normalization(dbpa_lock_cpu)
    # monitor_lock_cpu = normalization(monitor_lock_cpu)
    # print("Con_Commit + lock_single_anomaly:",
    #       TimeSeriesSimilarityImprove(np.array(dbpa_lock_cpu), np.array(monitor_lock_cpu)))

    df1 = pd.read_excel(
        'D:/美团数据库异常检测/mysqlrc/mysqlrc/fault_injection/Benchmark_Dataset/dbpa_dataset_compare/missing.xlsx',
        index_col=0)
    dbpa_lock_cpu = df1.T['Cpu User'].tolist()

    df2 = pd.read_excel(
        'D:/美团数据库异常检测/mysqlrc/mysqlrc/fault_injection/Benchmark_Dataset/dbpa_dataset_compare/missing_index-monitor.xlsx',
        index_col=0)
    monitor_lock_cpu = df2.T['cpu User'].tolist()[58:58 + len(dbpa_lock_cpu)]
    dbpa_lock_cpu = normalization(dbpa_lock_cpu)
    monitor_lock_cpu = normalization(monitor_lock_cpu)
    r, p = stats.pearsonr(np.array(dbpa_lock_cpu), np.array(monitor_lock_cpu))
    print("Cpu_User + missing_single_anomaly:", r)

    df1 = pd.read_excel(
        'D:/美团数据库异常检测/mysqlrc/mysqlrc/fault_injection/Benchmark_Dataset/dbpa_dataset_compare/missing+lock.xlsx',
        index_col=0)
    dbpa_lock_cpu = df1.T['Cpu User'].tolist()

    df2 = pd.read_excel(
        'D:/美团数据库异常检测/mysqlrc/mysqlrc/fault_injection/Benchmark_Dataset/dbpa_dataset_compare/record_lock+missing_index-monitor.xlsx',
        index_col=0)
    monitor_lock_cpu = df2.T['cpu User'].tolist()[58:58 + len(dbpa_lock_cpu)]
    dbpa_lock_cpu = normalization(dbpa_lock_cpu)
    monitor_lock_cpu = normalization(monitor_lock_cpu)
    r, p = stats.pearsonr(np.array(dbpa_lock_cpu), np.array(monitor_lock_cpu))
    print("Cpu_User + lock_missing_multi_anomaly:", r)

    # print(TimeSeriesSimilarityImprove(s1, s3))
    # print(TimeSeriesSimilarityImprove(s1, s4))
    # print(TimeSeriesSimilarityImprove(s1, s5))
