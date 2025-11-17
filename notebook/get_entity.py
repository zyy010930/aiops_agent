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

    # 如果基线平均值大于40，则将阈值调低
    if baseline_avg > 40:
        threshold = 1.25

    # 判断是否异常：正常时段平均值明显高于基线
    is_anomaly = normal_avg > baseline_avg * threshold and pre_avg < normal_avg and post_avg < normal_avg

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


def get_info(start_time, end_time, Target_service):
    # 使用异常检测进行查询aggregate_node_cpu_usage
    query = f"""
    .entity_set with(domain='k8s', name='k8s.pod', query=`pod_ip = ''10.53.56.8'' and pod=''fraud-detection-5df5cbfd4c-vbmh6'' and namespace = ''cms-demo''`)
    | entity-call get_metric('k8s', 'k8s.metric.high_level_metric_pod', 'pod_network_receive_rate', 'range', '1m')
    """

    print(f"🔍 Query: {query.strip()}")

    try:
        cpu = 0.0
        result = cms_tester._execute_spl_query(
            query.strip(),
            from_time=start_time,
            to_time=end_time
        )
        print(result)
    except Exception as e:
        print(f"❌ 异常检测过程中出错: {e}")
        return False


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


def analyze_cpu(normal_start, normal_end, Target_service, show):
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

    # 2. CMS查询语句（查询deployment的CPU总使用率）
    query_template = f"""
        .entity_set with(domain='k8s', name='k8s.deployment', query=`deployment='{Target_service}'`)
        | entity-call get_metric('k8s', 'k8s.metric.high_level_metric_deployment', 'deployment_cpu_usage_vs_requests', 'range', '1m')
        """

    # 3. 分别查询三个时段的数据
    result = cms_tester._execute_spl_query(
        query_template.strip(),
        from_time=pre10_start,
        to_time=post10_end
    )

    timestamps, cpu = get_result(result)

    # 4. 分割三个时段的数据
    pre10_end_dt = datetime.fromtimestamp(pre10_end)
    normal_end_dt = datetime.fromtimestamp(normal_end_ts)
    pre_values, normal_values, post_values = split_time_period_data(
        timestamps, cpu, pre10_end_dt, normal_end_dt
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
        print(f"🔴 异常检测: 检测时段cpu明显高于前后时段!")
    else:
        print(f"🟢 异常检测: 检测时段cpu处于正常范围")

    if show:
        plt.figure(figsize=(12, 6))

        plt.plot(timestamps, cpu, marker='o', linestyle='-', color='b')
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

    return is_anomaly, max_cpu, cpu


def analyze_memory(normal_start, normal_end, Target_service, show):
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

    # 2. CMS查询语句（查询deployment的CPU总使用率）
    query_template = f"""
        .entity_set with(domain='k8s', name='k8s.deployment', query=`deployment='{Target_service}'`)
        | entity-call get_metric('k8s', 'k8s.metric.high_level_metric_deployment', 'deployment_memory_usage_vs_limits', 'range', '1m')
        """

    # 3. 分别查询三个时段的数据
    result = cms_tester._execute_spl_query(
        query_template.strip(),
        from_time=pre10_start,
        to_time=post10_end
    )

    timestamps, memory = get_result(result)
    print(timestamps)

    # 4. 分割三个时段的数据
    pre10_end_dt = datetime.fromtimestamp(pre10_end)
    normal_end_dt = datetime.fromtimestamp(normal_end_ts)
    pre_values, normal_values, post_values = split_time_period_data(
        timestamps, memory, pre10_end_dt, normal_end_dt
    )

    # 5. 异常检测
    is_anomaly, normal_avg, pre_avg, post_avg = detect_anomaly(
        normal_values, pre_values, post_values
    )
    max_memory = max(normal_values)

    # 6. 输出异常检测结果
    print(f"\nmemory异常检测结果:")
    print(f"前10分钟平均值: {pre_avg:.4f}")
    print(f"检测时段平均值: {normal_avg:.4f}")
    print(f"后10分钟平均值: {post_avg:.4f}")
    print(f"最大memory使用率: {max_memory:.4f}")

    if is_anomaly:
        print(f"🔴 异常检测: 检测时段memory明显高于前后时段!")
    else:
        print(f"🟢 异常检测: 检测时段memory处于正常范围")

    if show:
        plt.figure(figsize=(12, 6))

        plt.plot(timestamps, memory, marker='o', linestyle='-', color='b')
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

    return is_anomaly, max_memory, memory

def get_pod_metrics(normal_start, normal_end, Target_pod, show):
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

    # 2. CMS查询语句（查询deployment的CPU总使用率）
    query_cpu_template = f"""
            .entity_set with(domain='k8s', name='k8s.pod', query=`name='{Target_pod}'`)
            | entity-call get_golden_metrics('range', '1m')
            """

    # k8s.event.events
    # 3. 分别查询三个时段的数据
    result = cms_tester._execute_spl_query(
        query_cpu_template.strip(),
        from_time=pre10_start,
        to_time=post10_end
    )
    print(result)
    data_list = result.data
    if not data_list:
        print(f"⚠️ 结果中 'data' 字段为空")
        return True, []

    timestamps = data_list[0][0]
    timestamps = eval(timestamps)
    print(timestamps)
    timestamps = [
        datetime.fromtimestamp(float(ts) / 1e9)  # 纳秒转秒（除以10^9）
        for ts in timestamps
    ]
    print("timestamps:", timestamps)
    # 原始逻辑：获取cpu数据
    cpu = data_list[0][2]
    cpu = eval(cpu)
    # 这里根据需要返回实际数据（示例）
    # 4. 分割三个时段的数据
    pre10_end_dt = datetime.fromtimestamp(pre10_end)
    normal_end_dt = datetime.fromtimestamp(normal_end_ts)
    pre_values, normal_values, post_values = split_time_period_data(
        timestamps, cpu, pre10_end_dt, normal_end_dt
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
    print(f"最大cpu使用率: {max_cpu:.4f}")

    if is_anomaly:
        print(f"🔴 异常检测: 检测时段cpu明显高于前后时段!")
    else:
        print(f"🟢 异常检测: 检测时段cpu处于正常范围")

    if show:
        plt.figure(figsize=(12, 6))

        plt.plot(timestamps, cpu, marker='o', linestyle='-', color='b')
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

    return cpu

def get_pod(normal_start, normal_end, Target_pod, show):
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

    # 2. CMS查询语句（查询deployment的CPU总使用率）
    query_cpu_template = f"""
            .entity_set with(domain='k8s', name='k8s.pod', query=`name='{Target_pod}'`)
            | entity-call get_golden_metrics('range', '1m')
            """

    # k8s.event.events
    # 3. 查询数据
    result = cms_tester._execute_spl_query(
        query_cpu_template.strip(),
        from_time=pre10_start,
        to_time=post10_end
    )
    print(result)
    data_list = result.data
    if not data_list:
        print(f"⚠️ 结果中 'data' 字段为空")
        return True, []

    ts_str = data_list[0][0]

    # 4. 计算查询时间段的总长度（分钟）
    total_duration_minutes = (post10_end - pre10_start) // 60  # 总秒数转分钟
    expected_points = total_duration_minutes  # 1分钟间隔，理论数据点数量

    # 5. 获取实际时间戳数据长度（假设ts_str是时间戳列表的字符串表示，需解析）
    # 注意：这里需要根据实际ts_str的格式调整解析方式
    # 示例：如果ts_str是"[1620000000, 1620000600, ...]"，则用eval转换为列表
    try:
        ts_list = eval(ts_str)  # 解析字符串为列表
        actual_points = len(ts_list)
    except:
        print(f"⚠️ 无法解析ts_str: {ts_str}")
        return False, []  # 解析失败也返回False

    # 6. 判断实际数据点是否少于预期
    if actual_points < expected_points:
        print(f"⚠️ 数据点不完整：预期{expected_points}个，实际{actual_points}个")
        return False, []

    # 原始逻辑：获取cpu数据
    cpu_str = data_list[0][2]
    # 这里根据需要返回实际数据（示例）
    return True, [ts_list, cpu_str]


if __name__ == "__main__":
    serveice_list = []
    problem_id = "129"
    input_data = read_input_data("../B榜题目.jsonl")
    for problem_data in input_data:
        if problem_data.get("problem_id") == problem_id:
        # if True:
            print(f"🔍 Found problem {problem_id}, processing...")
            problem_id = problem_data.get("problem_id", "unknown")
            time_range = problem_data.get("time_range", "")
            candidate_root_causes = problem_data.get("candidate_root_causes", [])
            alarm_rules = problem_data.get("alarm_rules", [])

            start_str, end_str = time_range.split(' ~ ')
            normal_start = datetime.strptime(start_str.strip(), "%Y-%m-%d %H:%M:%S")
            normal_end = datetime.strptime(end_str.strip(), "%Y-%m-%d %H:%M:%S")
            print(f"⏰ 正常时段: {normal_start} ~ {normal_end}")

            target_service = "payment"
            show = True
            cpu_anomaly = analyze_cpu(normal_start, normal_end, target_service, show)
            memory_anomaly = analyze_memory(normal_start, normal_end, target_service, show)
            print(f"CPU异常: {cpu_anomaly}, Memory异常: {memory_anomaly}")
            if cpu_anomaly[0]:
                print(f"存在异常")
            else:
                print(f"✅ 正常")
            break
