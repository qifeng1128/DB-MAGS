import datetime
import threading
import subprocess
import time
import sys
import getopt
import os


def case_make(duration_time, start_time, fault_t, tpcc_t, cause_fault_number, effect_fault_number, cause_fault_time):
    def execute_command_no_out(command):
        subprocess.call(command, shell=True, stdout=subprocess.DEVNULL)
    def execute_command(command):
        subprocess.call(command, shell=True)


    # 获取文件存储路径，将故障注入的根因写入文件夹

    now_time = datetime.datetime.now()
    now_str = f'{now_time.month} {now_time.day} {now_time.hour} {now_time.minute} {now_time.second}'
    print("time", now_str)
    path = "/root/mysqlrc/knowledge_graph/monitor/prometheus/" + str(now_time.month) + "-" + str(now_time.day) + "-" + str(now_time.hour) + "-" + str(now_time.minute) + "-" + str(now_time.second)

    print("path:", path)

    # 故障注入
    thread1 = threading.Thread(target=execute_command,
                               args=(
                               f'python3 /root/mysqlrc/fault_injection/tpcc.py -d {duration_time} -t {start_time} -i {fault_t} -s {tpcc_t} -c {cause_fault_number} -e {effect_fault_number} -p {path} -x {cause_fault_time}',))
    thread1.start()
    time.sleep(30)

    # now_time = datetime.datetime.now()
    # now_str = f'{now_time.month} {now_time.day} {now_time.hour} {now_time.minute} {now_time.second}'
    # print("time", now_str)


    # 采集数据
    thread2 = threading.Thread(target=execute_command_no_out,
                               args=('python3 /root/mysqlrc/knowledge_graph/Knowledge_graph/mysql_sys.py ' + now_str,))
    thread2.start()

    time.sleep(90)


    thread1.join()
    thread2.join()
    print("collect data------")

    # 采集数据
    thread3 = threading.Thread(target=execute_command_no_out,
                               args=(
                               'python3 /root/mysqlrc/knowledge_graph/monitor/prometheus/data_collect.py ' + now_str,))
    thread3.start()
    thread3.join()
    print("over")

def main(argv):
    duration = 0
    fault_inject_time = 0
    sleep_time = 0
    fault_type = 0
    total_cause_fault_number = 0
    total_effect_fault_number = 0
    total_data_number = 1
    cause_inject_time = 30
    try:
        # 处理传入的参数内容
        opts, args = getopt.getopt(argv, "hd:d:t:x:i:s:c:e:n:")
    except getopt.GetoptError:
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-d':
            duration = int(arg)
        if opt == '-t':
            fault_inject_time = int(arg)
        if opt == '-i':
            fault_type = int(arg)
        if opt == '-s':
            sleep_time = float(arg)
        if opt == '-c':
            total_cause_fault_number = int(arg)
        if opt == '-e':
            total_effect_fault_number = int(arg)
        if opt == '-n':
            total_data_number = int(arg)
        if opt == '-x':
            cause_inject_time = int(arg)

    for i in range(int(total_data_number)):
        print("start---------")
        case_make(duration, fault_inject_time, fault_type, sleep_time, total_cause_fault_number, total_effect_fault_number,
                  cause_inject_time)
        time.sleep(120)

if __name__ == "__main__":
    # -d 正常tpcc负载持续运行时间(s)
    # -t 注入果关系故障的时刻(一般设置为第60s)
    # -x 注入因关系故障的时刻（一般设置为第30s）
    # -i 故障注入的类型
    # -s 每个任务提交之后的休眠时间(s)
    # -c 故障注入任务的数量（为因关系的故障）
    # -e 故障注入任务的数量（为果关系的故障 / 单一类型故障）
    # -n 采集数据集的大小

    # args = ['-d', '100000', '-t', '60', '-x', 30, '-i', '5', '-s', '0.044', '-c', '1', '-e', '10', '-n', '1']
    # main(args)
    main(sys.argv[1:])
