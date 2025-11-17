import ast
import json
import os
import sys
import time
from datetime import datetime, timedelta

import numpy as np
from aliyun.log import LogClient, GetLogsRequest
from matplotlib import pyplot as plt


sys.path.append('..')
# SLS configuration
PROJECT_NAME = "proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao"
LOGSTORE_NAME = "logstore-tracing"
REGION = "cn-qingdao"

# Environment variables
STS_ROLE_ARN = os.getenv('ALIBABA_CLOUD_ROLE_ARN', 'acs:ram::1672753017899339:role/tianchi-user-a')
STS_SESSION_NAME = os.getenv('ALIBABA_CLOUD_ROLE_SESSION_NAME', 'my-sls-access')

# CMS 指标配置
CMS_WORKSPACE = "tianchi-workspace"
CMS_ENDPOINT = os.getenv("CMS_ENDPOINT", "cms.cn-qingdao.aliyuncs.com")
try:
    from test_cms_query import TestCMSQuery

    print("✅ TestCMSQuery imported successfully")
except ImportError as e:
    print(f"⚠️ Warning: Could not import TestCMSQuery: {e}")
    print("💡 Please install required dependencies: pip install -r requirements.txt")
    TestCMSQuery = None
# 初始化CMS测试客户端，用于指标查询
# 如果存在导入问题通过直接创建类修复(except)
try:
    if TestCMSQuery is not None:
        cms_tester = TestCMSQuery()
        cms_tester.setUp()
        print(f"✅ 已通过导入的 TestCMSQuery 初始化CMS客户端")
    else:
        raise ImportError("TestCMSQuery is None")
except:
    print("⚠️  TestCMSQuery import failed, creating CMS client directly...")

    import os
    from alibabacloud_cms20240330.client import Client as Cms20240330Client
    from alibabacloud_tea_openapi import models as open_api_models
    from alibabacloud_cms20240330 import models as cms_20240330_models
    from alibabacloud_tea_util import models as util_models


    class DirectCMSClient:
        def __init__(self):
            self.access_key_id = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID')
            self.access_key_secret = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
            self.workspace = CMS_WORKSPACE
            self.endpoint = CMS_ENDPOINT

            if not self.access_key_id or not self.access_key_secret:
                raise ValueError("请设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET")

            config = open_api_models.Config(
                access_key_id=self.access_key_id,
                access_key_secret=self.access_key_secret,
            )
            config.endpoint = self.endpoint
            self.cms_client = Cms20240330Client(config)

        def _execute_spl_query(self, query: str, from_time: int = None, to_time: int = None):
            """执行SPL查询"""
            if from_time is None:
                from_time = int(time.time()) - 60 * 60 * 1
            if to_time is None:
                to_time = int(time.time())

            try:
                headers = cms_20240330_models.GetEntityStoreDataHeaders()
                request = cms_20240330_models.GetEntityStoreDataRequest(
                    query=query,
                    from_=from_time,
                    to=to_time
                )
                runtime = util_models.RuntimeOptions()
                response = self.cms_client.get_entity_store_data_with_options(
                    self.workspace, request, headers, runtime
                )
                return response.body
            except Exception as e:
                print(f"❌ CMS查询错误: {e}")
                return None


    cms_tester = DirectCMSClient()
    print(f"✅ CMS client created directly")

print(f"🔧 CMS客户端已初始化")
print(f"🔧 workspace: {CMS_WORKSPACE}")
print(f"🔧 Endpoint: {CMS_ENDPOINT}")


def get_sts_credentials():
    try:
        from aliyunsdkcore.client import AcsClient
        from aliyunsdksts.request.v20150401 import AssumeRoleRequest

        MAIN_ACCOUNT_ACCESS_KEY_ID = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_ID')
        MAIN_ACCOUNT_ACCESS_KEY_SECRET = os.getenv('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
        ALIBABA_CLOUD_ROLE_ARN = os.getenv('ALIBABA_CLOUD_ROLE_ARN', 'acs:ram::1672753017899339:role/tianchi-user-a')
        STS_SESSION_NAME = os.getenv('ALIBABA_CLOUD_ROLE_SESSION_NAME', 'my-sls-access')

        if not MAIN_ACCOUNT_ACCESS_KEY_ID or not MAIN_ACCOUNT_ACCESS_KEY_SECRET:
            return None, None, None

        client = AcsClient(MAIN_ACCOUNT_ACCESS_KEY_ID, MAIN_ACCOUNT_ACCESS_KEY_SECRET, REGION)
        request = AssumeRoleRequest.AssumeRoleRequest()
        request.set_RoleArn(ALIBABA_CLOUD_ROLE_ARN)
        request.set_RoleSessionName(STS_SESSION_NAME)
        request.set_DurationSeconds(3600)

        response = client.do_action_with_exception(request)
        response_data = json.loads(response)
        credentials = response_data['Credentials']
        return (credentials['AccessKeyId'], credentials['AccessKeySecret'], credentials['SecurityToken'])
    except Exception as e:
        print(f"❌ 获取STS凭证失败: {e}")
        return None, None, None


temp_access_key_id, temp_access_key_secret, security_token = get_sts_credentials()
if not temp_access_key_id:
    print("❌ 无法获取STS临时凭证，分析终止")

try:
    from aliyun.log import LogClient

    sls_endpoint = os.getenv("SLS_ENDPOINT", "cn-qingdao.log.aliyuncs.com")
    log_client = LogClient(sls_endpoint, temp_access_key_id, temp_access_key_secret, security_token)
except Exception as e:
    print(f"❌ 创建SLS客户端失败: {e}")


def read_input_data(input_file_path):
    """
    Read and parse input data from JSONL file

    Args:
        input_file_path: Path to the input JSONL file

    Returns:
        list: List of parsed JSON objects
    """
    data = []

    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        item = json.loads(line)
                        data.append(item)
                    except json.JSONDecodeError as e:
                        print(f"⚠️ Failed to parse line: {line[:100]}... Error: {e}")
                        continue

        print(f"✅ Successfully read {len(data)} records from {input_file_path}")
        return data

    except FileNotFoundError:
        print(f"❌ Input file not found: {input_file_path}")
        return []
    except Exception as e:
        print(f"❌ Failed to read input file: {e}")
        return []


def detect_anomaly(normal_values, pre_values, post_values, threshold=1.5):
    """
    检测正常时段的指标是否明显高于前后时段

    Args:
        normal_values: 正常时段的指标值列表
        pre_values: 前10分钟的指标值列表
        post_values: 后10分钟的指标值列表
        threshold: 异常阈值，正常时段平均值超过前后时段平均值的倍数

    Returns:
        tuple: (是否异常, 正常时段平均值, 前时段平均值, 后时段平均值)
    """
    if not normal_values or not pre_values or not post_values:
        print("⚠️ 缺少数据，无法进行异常检测")
        return False, 0, 0, 0

    # 计算各时段平均值
    normal_avg = np.mean(normal_values)
    pre_avg = np.mean(pre_values)
    post_avg = np.mean(post_values)

    # 计算前后时段的平均水平
    baseline_avg = np.mean([pre_avg, post_avg])

    # 判断是否异常：正常时段平均值明显高于基线
    is_anomaly = (normal_avg > baseline_avg * threshold or normal_avg > baseline_avg + 20) and pre_avg < normal_avg and post_avg < normal_avg

    return is_anomaly, normal_avg, pre_avg, post_avg


def split_time_period_data(timestamps, values, pre10_end, normal_end):
    """
    将数据按时间分割为前10分钟、正常时段、后10分钟

    Args:
        timestamps: 时间戳列表
        values: 指标值列表
        pre10_end: 前10分钟结束时间
        normal_end: 正常时段结束时间

    Returns:
        tuple: (前10分钟值列表, 正常时段值列表, 后10分钟值列表)
    """
    pre_values = []
    normal_values = []
    post_values = []

    for ts, val in zip(timestamps, values):
        if ts <= pre10_end:
            pre_values.append(val)
        elif ts <= normal_end:
            normal_values.append(val)
        else:
            post_values.append(val)

    return pre_values, normal_values, post_values

def get_result(result):
    # 1. 从result中提取data列表（原始结果是字典，直接用键访问）
    # 注意：根据你的打印结果，result是字典，不是对象，所以用['data']而非.result.data
    data_list = result.data
    if not data_list:
        print(f"⚠️ 结果中 'data' 字段为空")
        return [], []

    # 2. 提取时间戳列表（__ts__列，对应data_list[0][2]）和CPU值列表（__value__列，对应data_list[0][3]）
    # data_list[0]是第一行数据，[2]是第三列（__ts__），[3]是第四列（__value__）
    ts_str = data_list[0][2]  # 格式："[1758037443000000000, 1758037503000000000, ...]"
    cpu_str = data_list[0][3]  # 格式："[0.00231595..., 0.01094574..., ...]"

    # 3. 将字符串列表转为Python列表（用ast.literal_eval，避免eval的安全风险）
    ts_list = ast.literal_eval(ts_str)  # 时间戳列表（单位：纳秒）
    cpu_list = ast.literal_eval(cpu_str)  # CPU数值列表

    # 4. 处理时间戳：纳秒转秒（除以1e9），再转为datetime格式
    timestamps = []
    for ts in ts_list:
        # 纳秒 → 秒（除以10^9），再转为datetime
        dt = datetime.fromtimestamp(ts / 1e9)
        timestamps.append(dt)

    # 5. 确保CPU值为float类型（防止原始数据是字符串）
    cpu_values = [float(val) for val in cpu_list]

    return timestamps, cpu_values


def analyze_network(normal_start, normal_end, Target_ECS, show):
    # 1. 计算三个时段的时间戳（转为int类型，CMS查询要求）
    # 前10分钟：normal_start - 10min 到 normal_start
    pre10_start = int((normal_start - timedelta(minutes=10)).timestamp())
    pre10_end = int(normal_start.timestamp())
    # 正常时段：normal_start 到 normal_end
    normal_start_ts = int(normal_start.timestamp())
    normal_end_ts = int(normal_end.timestamp())
    # 后10分钟：normal_end 到 normal_end + 10min
    post10_start = int(normal_end.timestamp())
    post10_end = int((normal_end + timedelta(minutes=10)).timestamp())

    # 2. 查询语句
    query_list = []
    query_list.append(f"""
    .metricstore with(project='workspace-tianchi-2025-0828-01', metricstore='aliyun-prom-rw-16e081404ce19b5294c7967ff61d')
    | prom-call promql_query_range('sum(irate(node_netstat_Tcp_OutRsts{{instanceId=~"{Target_ECS}"}}[1m]))', '60s')
    """)

    query_list.append(f"""
    .metricstore with(project='workspace-tianchi-2025-0828-01', metricstore='aliyun-prom-rw-16e081404ce19b5294c7967ff61d')
    | prom-call promql_query_range('sum(irate(node_netstat_TcpExt_TCPSynRetrans{{instanceId=~"{Target_ECS}"}}[1m]))', '60s')
    """)

    query_list.append(f"""
    .metricstore with(project='workspace-tianchi-2025-0828-01', metricstore='aliyun-prom-rw-16e081404ce19b5294c7967ff61d')
    | prom-call promql_query_range('sum(irate(node_netstat_Tcp_RetransSegs{{instanceId=~"{Target_ECS}"}}[1m]))', '60s')
    """)

    query_list.append(f"""
    .metricstore with(project='workspace-tianchi-2025-0828-01', metricstore='aliyun-prom-rw-16e081404ce19b5294c7967ff61d')
    | prom-call promql_query_range('sum(irate(node_netstat_Tcp_InErrs{{instanceId=~"{Target_ECS}"}}[1m]))', '60s')
    """)

    anomalyNum = 0
    for query in query_list:
        result = cms_tester._execute_spl_query(
            query.strip(),
            from_time=pre10_start,
            to_time=post10_end
        )

        timestamps, network = get_result(result)
        if len(timestamps) == 0:
            continue

        # 4. 分割三个时段的数据
        pre10_end_dt = datetime.fromtimestamp(pre10_end)
        normal_end_dt = datetime.fromtimestamp(normal_end_ts)
        pre_values, normal_values, post_values = split_time_period_data(
            timestamps, network, pre10_end_dt, normal_end_dt
        )

        # 5. 异常检测
        is_anomaly, normal_avg, pre_avg, post_avg = detect_anomaly(
            normal_values, pre_values, post_values
        )
        max_cpu = max(normal_values)

        # 6. 输出异常检测结果
        print(f"\ncpu异常检测结果:")
        print(f"前10分钟平均值: {pre_avg:.4f}")
        print(f"检测时段平均值: {normal_avg:.4f}")
        print(f"后10分钟平均值: {post_avg:.4f}")
        print(f"最大CPU使用率: {max_cpu:.4f}")

        if is_anomaly:
            print(f"🔴 异常检测: 检测时段network明显高于前后时段!")
            anomalyNum += 1
        else:
            print(f"🟢 异常检测: 检测时段network处于正常范围")

        if show:
            plt.figure(figsize=(12, 6))

            plt.plot(timestamps, network, marker='o', linestyle='-', color='b')
            pre10_start_dt = datetime.fromtimestamp(pre10_start)  # 前10分钟开始（datetime）
            pre10_end_dt = datetime.fromtimestamp(pre10_end)  # 前10分钟结束（datetime）
            normal_start_dt = datetime.fromtimestamp(normal_start_ts)  # 目标时段开始（datetime）
            normal_end_dt = datetime.fromtimestamp(normal_end_ts)  # 目标时段结束（datetime）
            post10_start_dt = datetime.fromtimestamp(post10_start)  # 后10分钟开始（datetime）
            post10_end_dt = datetime.fromtimestamp(post10_end)  # 后10分钟结束（datetime）
            # 用阴影标记三个时段
            plt.axvspan(pre10_start_dt, pre10_end_dt, color='lightgreen', alpha=0.3, label='前10分钟')
            plt.axvspan(normal_start_dt, normal_end_dt, color='lightcoral', alpha=0.3, label='目标时段')
            plt.axvspan(post10_start_dt, post10_end_dt, color='lightblue', alpha=0.3, label='后10分钟')

            plt.show()
    return anomalyNum

def analyze_gc(normal_start, normal_end, Target_ECS, show):
    # 1. 计算三个时段的时间戳（转为int类型，CMS查询要求）
    # 前10分钟：normal_start - 10min 到 normal_start
    pre10_start = int((normal_start - timedelta(minutes=10)).timestamp())
    pre10_end = int(normal_start.timestamp())
    # 正常时段：normal_start 到 normal_end
    normal_start_ts = int(normal_start.timestamp())
    normal_end_ts = int(normal_end.timestamp())
    # 后10分钟：normal_end 到 normal_end + 10min
    post10_start = int(normal_end.timestamp())
    post10_end = int((normal_end + timedelta(minutes=10)).timestamp())

    # 2. 查询语句
    # query= f"""
    # .metricstore with(project='workspace-tianchi-2025-0828-01', metricstore='aliyun-prom-arms-67b1a0473064fa06ae361d42ad')
    # | prom-call promql_query_range('sum (sum_over_time_lorc(arms_jvm_gc_delta{{acs_arms_service_id="hwx28v3j7p@680213ea70b15a61c56ed",gen="old",host=~".*", }}[1m]))', '60s')
    # """
    query = f"""
    .entity_set with(domain='apm', name='apm.service', query=`service='inventory'`)
    | entity-call get_metric('apm', 'apm.metric.jvm', 'arms_jvm_gc_delta', 'range', '1m')
    """

    anomalyNum = 0
    result = cms_tester._execute_spl_query(
        query.strip(),
        from_time=pre10_start,
        to_time=post10_end
    )
    print(result)

    timestamps, network = get_result(result)

    # 4. 分割三个时段的数据
    pre10_end_dt = datetime.fromtimestamp(pre10_end)
    normal_end_dt = datetime.fromtimestamp(normal_end_ts)
    pre_values, normal_values, post_values = split_time_period_data(
        timestamps, network, pre10_end_dt, normal_end_dt
    )

    # 5. 异常检测
    is_anomaly, normal_avg, pre_avg, post_avg = detect_anomaly(
        normal_values, pre_values, post_values
    )
    max_gc = max(normal_values)

    # 6. 输出异常检测结果
    print(f"\ncpu异常检测结果:")
    print(f"前10分钟平均值: {pre_avg:.4f}")
    print(f"检测时段平均值: {normal_avg:.4f}")
    print(f"后10分钟平均值: {post_avg:.4f}")
    print(f"最大gc次数: {max_gc:.4f}")

    if is_anomaly:
        print(f"🔴 异常检测: 检测时段gc明显高于前后时段!")
        return True
    else:
        print(f"🟢 异常检测: 检测时段gc处于正常范围")

    if show:
        plt.figure(figsize=(12, 6))

        plt.plot(timestamps, network, marker='o', linestyle='-', color='b')
        pre10_start_dt = datetime.fromtimestamp(pre10_start)  # 前10分钟开始（datetime）
        pre10_end_dt = datetime.fromtimestamp(pre10_end)  # 前10分钟结束（datetime）
        normal_start_dt = datetime.fromtimestamp(normal_start_ts)  # 目标时段开始（datetime）
        normal_end_dt = datetime.fromtimestamp(normal_end_ts)  # 目标时段结束（datetime）
        post10_start_dt = datetime.fromtimestamp(post10_start)  # 后10分钟开始（datetime）
        post10_end_dt = datetime.fromtimestamp(post10_end)  # 后10分钟结束（datetime）
        # 用阴影标记三个时段
        plt.axvspan(pre10_start_dt, pre10_end_dt, color='lightgreen', alpha=0.3, label='前10分钟')
        plt.axvspan(normal_start_dt, normal_end_dt, color='lightcoral', alpha=0.3, label='目标时段')
        plt.axvspan(post10_start_dt, post10_end_dt, color='lightblue', alpha=0.3, label='后10分钟')

        plt.show()
    return False

if __name__ == "__main__":
    serveice_list = []
    problem_id = "151"
    input_data = read_input_data("../B榜题目.jsonl")
    anomaly_list = []
    for problem_data in input_data:
        problem_id = problem_data.get("problem_id", "unknown")
        problem_id = "055"
        time_range = problem_data.get("time_range", "")
        candidate_root_causes = problem_data.get("candidate_root_causes", [])
        alarm_rules = problem_data.get("alarm_rules", [])
        start_str, end_str = time_range.split(' ~ ')
        normal_start = datetime.strptime(start_str.strip(), "%Y-%m-%d %H:%M:%S")
        normal_end = datetime.strptime(end_str.strip(), "%Y-%m-%d %H:%M:%S")
        # if problem_data.get("alarm_rules")[0] == 'greyFailure':
        if problem_data.get("problem_id") == problem_id:
            anomaly = analyze_gc(normal_start, normal_end, "inventory", False)
            if anomaly:
                anomaly_list.append(problem_id)
            # for candidate in candidate_root_causes:
            #     if '.' in candidate and candidate.endswith('.cpu'):
            #         service = candidate.split('.')[0]
            #         if service[1] != '-':
            #             continue
            #
            #         anomaly = analyze_network(normal_start, normal_end, service, False)
            #         if anomaly >= 2:
            #             anomaly_list.append({problem_id, service})
    print(anomaly_list)
