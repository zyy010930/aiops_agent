import json
import os
import argparse
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
from openai import OpenAI

from get_entity import analyze_cpu, analyze_memory, get_pod
from get_log import read_input_data, get_log, get_span_latency
from get_ecs import analyze_ecs_memory, analyze_ecs_cpu, analyze_ecs_disk
from get_error import get_error, get_span_error, get_errorInfo
from get_instance import get_instance
from get_prom import analyze_network, analyze_gc

# SLS configuration
PROJECT_NAME = "proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao"
LOGSTORE_NAME = "logstore-tracing"
REGION = "cn-qingdao"

def call_bailian_model(root_causes: List[str], root_cause_data: Dict[str, Any]) -> str:
    final_root_causes = []
    """
    调用阿里云百炼大模型接口，从多个根因中筛选最可能的结果
    """
    client = OpenAI(
        # 若没有配置环境变量，请用百炼API Key将下行替换为：api_key="sk-xxx",
        api_key=os.getenv("BAILIAN_API_KEY"),
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )

    # 构造根因数据字符串
    cause_data_str = ""
    for cause in root_causes:
        if cause in root_cause_data:
            cause_data_str += f"- {cause} 的详细数据: {json.dumps(root_cause_data[cause], ensure_ascii=False, default=str)}\n"

    # 构造提示词，包含根因数据，由于数据采样包含异常时间段的前十分钟和后十分钟，因此提示词中需要说明异常时间段帮助选择出最符合时间的根因。
    prompt = f"""
           请根据以下信息从候选根因中选择最可能的一个：

           候选根因列表（格式为"服务名.故障类型"）：
           {json.dumps(root_causes, ensure_ascii=False)}

           各候选根因的详细数据：
           {cause_data_str}
           
           异常上升趋势开始于第十一个点以后的更可能是根因，异常上升开始于第十个点之前的可以降低优先级，异常延续到倒数第六个点结束的更可能是根因，早于该点的可以降低优先级。

           请结合根因的数据，根据异常时间起点和根因数据异常实际起点，分析哪个根因最可能是问题的源头。
           要求：只返回选中的根因字符串，不要额外解释。
           """
    print(f"prompt: {prompt}")

    completion = client.chat.completions.create(
        model="kimi-k2-thinking",
        messages=[
            {"role": "system", "content": "你是一个分布式系统故障诊断专家"},
            {"role": "user", "content": f"{prompt}"},
        ],
        stream=True
    )
    causes = ""
    for chunk in completion:
        if chunk.choices[0].delta.content:
            print(chunk.choices[0].delta.content, end="", flush=True)
            causes += chunk.choices[0].delta.content

    return causes.strip()

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

    sls_endpoint = f"{REGION}.log.aliyuncs.com"
    log_client = LogClient(sls_endpoint, temp_access_key_id, temp_access_key_secret, security_token)
except Exception as e:
    print(f"❌ 创建SLS客户端失败: {e}")

# 定义所有调用关系：(调用方, 被调用方)
calls_relations = [
    ("load-generator", "frontend-proxy"),
    ("frontend-web", "frontend-proxy"),
    ("frontend-proxy", "image-provider"),
    ("frontend-proxy", "frontend"),
    ("frontend", "ad"),
    ("frontend", "recommendation"),
    ("frontend", "product-catalog"),
    ("frontend", "checkout"),
    ("frontend", "cart"),
    ("frontend", "currency"),
    ("recommendation", "product-catalog"),
    ("cart", "inventory"),
    ("checkout", "product-catalog"),
    ("checkout", "cart"),
    ("checkout", "payment"),
    ("checkout", "shipping"),
    ("checkout", "email"),
    ("checkout", "currency"),
    ("shipping", "quote"),
]

# 初始化上游服务字典
service_upstreams = {}
all_services = set()

# 收集所有服务
for caller, callee in calls_relations:
    all_services.add(caller)
    all_services.add(callee)

# 过滤掉非应用服务（数据库、消息服务等）
app_services = [s for s in all_services if not (s.startswith("rm-") or s.startswith("r-") or s == "orders")]

# 初始化应用服务的上游列表
for service in app_services:
    service_upstreams[service] = []

# 填充上游应用
for caller, callee in calls_relations:
    if callee in app_services:  # 只记录应用服务的上游
        service_upstreams[callee].append(caller)


def get_only_anomaly(anomaly_list, root_causes, evidences_dict):
    amplitude_dict = {}
    for anomaly in anomaly_list:
        service = anomaly['service']
        if service + '.networkLatency' not in root_causes:
            continue
        # 假设before、target为数值列表，取平均值计算
        try:
            before = anomaly['before']
            after = anomaly['after']
            target = anomaly['target']
            amplitude = (target - before) / before + (target - after) / after  # 相对增幅
            amplitude_dict[service] = amplitude
            print(f"📊 {service} 上升幅度: {amplitude:.2f}x")
            # 记录证据
            evidence = f"{service}的网络延迟存在异常，异常值为{target}，相比正常区间前半段({before})和后半段({after})的增幅为{amplitude:.2f}x"
            evidences_dict[service + '.networkLatency'].append(evidence)
        except Exception as e:
            print(f"❌ 计算{service}幅度失败: {e}")

    if amplitude_dict:
        # 找到幅度最大的服务
        max_amplitude_service = max(amplitude_dict.items(), key=lambda x: x[1])[0]
        # 只保留该服务的根因
        root_causes = [item for item in root_causes if item.split('.')[0] == max_amplitude_service]
        # 添加筛选证据
        for cause in root_causes:
            evidences_dict[cause].append(f"通过计算异常幅度，{cause.split('.')[0]}的异常幅度最大，被选为主要根因")
        print(f"🎯 按最大上升幅度筛选后的根因: {root_causes}")
    return root_causes, evidences_dict


def get_frequency(cpu_list, memory_list, latency_candidates, jvm_list):
    # -------------------------- 新增统计逻辑 --------------------------
    # 1. 初始化计数器，用于统计每个service的出现次数
    service_counts = defaultdict(int)

    # 2. 从三个列表中提取service并统计频率
    # 处理cpu_list（元素格式："service.cpu"）
    for item in cpu_list:
        service = item.split('.')[0]  # 提取service名称
        service_counts[service] += 1

    # 处理memory_list（元素格式："service.memory"）
    for item in memory_list:
        service = item.split('.')[0]
        service_counts[service] += 1

    # 处理latency_candidates（元素格式："service.networkLatency"）
    for item in latency_candidates:
        service = item.split('.')[0]
        service_counts[service] += 1

    # 处理jvm_list（元素格式："service.jvm"）
    for item in jvm_list:
        service = item.split('.')[0]
        service_counts[service] += 1

    # 3. 筛选出现频率最高的service（若有多个则全部保留）
    if service_counts:  # 避免空列表导致的错误
        max_frequency = max(service_counts.values())  # 获取最高频率
        # 筛选所有频率等于最高频率的service
        most_frequent_services = [
            service for service, count in service_counts.items()
            if count == max_frequency
        ]
    else:
        most_frequent_services = []  # 若三个列表都为空，返回空列表

    # 4. 输出结果
    print("\n统计结果：")
    if most_frequent_services:
        print(f"出现频率最高的service(s)（频率：{max_frequency}）：{most_frequent_services}")
    else:
        print("三个列表均为空，无service可统计")

    return most_frequent_services


def find_anomalies(root_list, root_cause_data, m=5, threshold_factor=3, consecutive=3):
    """
    找出时间序列中异常的开始点、结束点和最高点

    参数:
        time_series (dict): 包含'cpu_data'和'memory_data'的字典，值为时间序列列表
        m (int): 计算基线差异的前m个点数量，默认5
        threshold_factor (int): 阈值倍数，默认3（即基线均值+3*标准差）
        consecutive (int): 连续多少个差异低于阈值视为异常结束，默认3

    返回:
        dict: 包含'cpu'和'memory'的异常信息，每个包含'start'（开始索引）、'end'（结束索引）、
              'peak'（最高点值）、'peak_index'（最高点索引）
    """
    results = {}
    seriesLen = 0
    for root_causes in root_list:
        data = root_cause_data[root_causes]['cpu_data']
        seriesLen = len(data)

        n = len(data)
        if n < 2:
            results[root_causes] = None  # 去掉'_data'后缀
            continue

        # 计算相邻数据点的绝对差异
        diff_abs = [abs(data[i + 1] - data[i]) for i in range(n - 1)]
        if len(diff_abs) < m:
            results[root_causes] = None
            continue

        # 计算基线差异的均值和标准差（前m个差异）
        baseline_diff = diff_abs[:m]
        mu_diff = np.mean(baseline_diff)
        sigma_diff = np.std(baseline_diff)
        start_threshold = mu_diff + threshold_factor * sigma_diff

        # 寻找异常开始点：第一个超过阈值的差异对应的后一个数据点
        start_idx = None
        for i in range(m, len(diff_abs)):  # 从第m个差异开始检查
            if diff_abs[i] > start_threshold:
                start_idx = i + 1  # 差异i对应data[i]到data[i+1]，异常开始于i+1
                break

        if start_idx is None:  # 无异常开始点
            results[root_causes] = {
                'start': None,
                'end': None,
                'peak': None,
                'peak_index': None
            }
            continue

        # 寻找异常结束点：连续consecutive个差异低于阈值时，取最后一个差异对应的后一个数据点
        end_idx = n - 1  # 默认结束于最后一个点
        current_consecutive = 0
        for i in range(start_idx - 1, len(diff_abs)):  # i是diff_abs的索引
            if diff_abs[i] <= start_threshold:
                current_consecutive += 1
                if current_consecutive >= consecutive:
                    end_idx = i + 1  # 结束点为i+1（data的索引）
                    break
            else:
                current_consecutive = 0  # 不连续则重置计数

        # 提取异常区间数据并找到最高点
        anomaly_data = data[start_idx:end_idx + 1]  # 包含end_idx
        if not anomaly_data:
            peak = None
            peak_index = None
        else:
            peak = max(anomaly_data)
            peak_index = start_idx + anomaly_data.index(peak)

        # 存储结果
        results[root_causes] = {
            'start': start_idx,
            'end': end_idx,
            'peak': peak,
            'peak_index': peak_index
        }
    print(results)
    final_list = []
    for root_causes, result in results.items():
        print(f"异常根因：{root_causes}")
        print(f"异常开始点索引：{result['start']}")
        print(f"异常结束点索引：{result['end']}")
        print(f"异常区间最高点值：{result['peak']}")
        print(f"异常区间最高点索引：{result['peak_index']}")
        # 判断异常上升区间是否在正常区间内，如果超出区间则忽略
        if (result['peak_index'] is not None and result['peak_index'] <= 12) or (
                result['start'] is not None and result['start'] <= 8):
            continue
        final_list.append(root_causes)

    return final_list


# 处理延迟问题
def analyze_latency_problem(normal_start, normal_end, candidate_root_causes):
    anomaly_list: List[Dict[str, Any]] = []
    show = False
    latency = False
    cpu_list = []
    memory_list = []
    serveice_list = []
    combined = []
    latency_candidates = []  # 临时存储所有latency候选服务
    root_cause_data = {}  # 新增：存储根因数据的字典
    evidences_dict = defaultdict(list)  # 存储每个根因的证据
    start_str = normal_start.replace(tzinfo=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
    end_str = normal_end.replace(tzinfo=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')

    def process_one_service(service, normal_start, normal_end, isMedian=True):
        result = {
            'service': service,
            'cpu_anomaly': False,
            'memory_anomaly': False,
            'latency_anomaly': False,
            'anomaly_data': None,
            'cpu_data': None,  # 存储CPU原始数据
            'max_cpu': 0,  # 存储最大CPU值
            'memory_data': None,  # 存储内存原始数据
            'max_memory': 0,  # 存储最大内存值
            'latency_data': None  # 存储延迟数据
        }
        print(f"🎯 Limiting analysis to candidate service: {service}")

        # 1. 查询CPU数据
        print(f"🔍 查询 {service} 服务CPU数据...")
        cpu_anomaly, max_cpu, cpu_data = analyze_cpu(normal_start, normal_end, service, show)
        result['cpu_data'] = cpu_data
        result['max_cpu'] = max_cpu
        if cpu_anomaly and max_cpu > 30.0:
            # 记录CPU异常证据
            evidences_dict[service + '.cpu'].append(
                f"{service}的CPU使用率出现异常，最大值达到{max_cpu}%"
            )
            result['cpu_anomaly'] = True

        # 2. 查询Memory数据
        print(f"🔍 查询 {service} 服务Memory数据...")
        if service == "email":
            result['memory_anomaly'] = False
            result['memory_data'] = []
            result['latency_anomaly'] = False
            result['latency_data'] = []
            return result

            # 2. 查询Memory数据
        memory_anomaly, max_memory, memory_data = analyze_memory(normal_start, normal_end, service, show)
        result['memory_data'] = memory_data
        result['max_memory'] = max_memory
        if memory_anomaly and max_memory > 25.0:
            # 记录内存异常证据
            evidences_dict[service + '.memory'].append(
                f"{service}的内存使用率出现异常，最大值达到{max_memory}%"
            )
            result['memory_anomaly'] = True

        # 3. 获取延迟数据
        print(f"🎯 Limiting analysis to candidate service: {service}")
        flag, before, target, after, duration_data = get_log(log_client, PROJECT_NAME, LOGSTORE_NAME, service, start_str.strip(),
                                              end_str.strip(), isMedian)
        result['latency_data'] = duration_data
        if flag:
            result['latency_anomaly'] = True
            result['anomaly_data'] = {
                "service": service,
                "before": before,
                "target": target,
                "after": after,
            }
            # 记录延迟异常证据
            evidences_dict[service + '.networkLatency'].append(
                f"{service}的网络延迟出现异常，异常值为{target}，正常区间前半段为{before}，后半段为{after}"
            )
        return result

    # 并行
    total_services = []
    for candidate in candidate_root_causes:
        if '.' in candidate and candidate.endswith('.cpu'):
            service = candidate.split('.')[0]
            if service[1] == '-' or service == "load-generator":
                continue
            total_services.append(service)

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_one_service, service, normal_start, normal_end) for service in total_services
        ]
        for future in as_completed(futures):
            result = future.result()
            service_name = result['service']
            if result['cpu_anomaly']:
                cpu_item = service_name + '.cpu'
                cpu_list.append(cpu_item)
                # 存储CPU根因数据
                root_cause_data[cpu_item] = {
                    'cpu_data': result['cpu_data'],
                    'memory_data': result['memory_data'],
                    'duration_data': result['latency_data']
                }
            if result['memory_anomaly']:
                memory_item = service_name + '.memory'
                memory_list.append(memory_item)
                # 存储内存根因数据
                root_cause_data[memory_item] = {
                    'cpu_data': result['cpu_data'],
                    'memory_data': result['memory_data'],
                    'duration_data': result['latency_data']
                }
            if result['latency_anomaly']:
                latency_item = service_name + '.networkLatency'
                latency_candidates.append(latency_item)
                anomaly_list.append(result['anomaly_data'])
                # 存储延迟根因数据
                root_cause_data[latency_item] = {
                    'duration_data': result['latency_data']
                }

    if cpu_list == [] and memory_list == [] and latency_candidates == []:
        print("放宽异常检测要求，改用平均值")
        total_services = []
        for candidate in candidate_root_causes:
            if '.' in candidate and candidate.endswith('.cpu'):
                service = candidate.split('.')[0]
                if service[1] == '-' or service == "load-generator":
                    continue
                total_services.append(service)

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(process_one_service, service, normal_start, normal_end, False) for service in
                total_services
            ]
            for future in as_completed(futures):
                result = future.result()
                service_name = result['service']
                if result['cpu_anomaly']:
                    cpu_item = service_name + '.cpu'
                    cpu_list.append(cpu_item)
                    # 存储CPU根因数据
                    root_cause_data[cpu_item] = {
                        'cpu_data': result['cpu_data'],
                        'memory_data': result['memory_data'],
                        'duration_data': result['latency_data']
                    }
                if result['memory_anomaly']:
                    memory_item = service_name + '.memory'
                    memory_list.append(memory_item)
                    # 存储内存根因数据
                    root_cause_data[memory_item] = {
                        'cpu_data': result['cpu_data'],
                        'memory_data': result['memory_data'],
                        'duration_data': result['latency_data']
                    }
                if result['latency_anomaly']:
                    latency_item = service_name + '.networkLatency'
                    latency_candidates.append(latency_item)
                    anomaly_list.append(result['anomaly_data'])
                    # 存储延迟根因数据
                    root_cause_data[latency_item] = {
                        'duration_data': result['latency_data']
                    }

    # 查询jvmChaos的情况
    jvm_list = []
    anomaly = analyze_gc(normal_start, normal_end, "inventory", False)
    if anomaly:
        jvm_list.append('inventory.jvmChaos')
        evidences_dict['inventory.jvmChaos'].append(
            "inventory服务检测到JVM GC异常，可能存在JVM Chaos问题"
        )

    fre = get_frequency(cpu_list, memory_list, latency_candidates, jvm_list)

    # 从latency候选服务中筛选最下游应用
    # 1. 提取候选服务中的应用名
    candidate_services = [item.split('.')[0] for item in latency_candidates]

    # 2. 构建候选服务之间的下游关系
    candidate_downstreams = {s: [] for s in candidate_services}
    for caller, callee in calls_relations:
        if caller in candidate_services and callee in candidate_services:
            candidate_downstreams[caller].append(callee)

    # 3. 筛选候选服务中没有下游的应用（最下游）
    most_downstream_in_candidates = [
        s for s in candidate_services
        if not candidate_downstreams[s]  # 下游列表为空
    ]

    # 4. 生成新的latency候选列表（只保留最下游应用）
    serveice_list = [
        item for item in latency_candidates
        if item.split('.')[0] in most_downstream_in_candidates
    ]

    # 5. 针对frontend应用，判断是否存在延迟
    for item in latency_candidates:
        if item.split('.')[0] == "frontend":
            service = item.split('.')[0]
            latency = get_span_latency(log_client, PROJECT_NAME, LOGSTORE_NAME, service, start_str.strip(), end_str.strip(), False)
            if latency:
                serveice_list = [service + '.networkLatency']
                evidences_dict[service + '.networkLatency'].append(
                    "frontend服务检测到Span延迟异常，被选为候选根因"
                )

    if len(cpu_list) > 1:
        cpu_list = find_anomalies(cpu_list, root_cause_data)

    print(f"🎯 cpu候选服务列表: {cpu_list}")
    print(f"🎯 memory候选服务列表: {memory_list}")
    print(f"🎯 latency候选服务列表: {serveice_list}")
    print(f"🎯 jvmChaos候选服务列表: {jvm_list}")

    # 综合判断根因
    # 1. 提取cpu和memory列表中的所有唯一服务
    cpu_services = {item.split('.')[0] for item in cpu_list}
    memory_services = {item.split('.')[0] for item in memory_list}
    all_non_latency_services = cpu_services.union(memory_services)

    # 2. 提取latency列表中的所有服务
    service_services = {item.split('.')[0] for item in serveice_list}

    # 3. 根据规则合并根因
    if len(service_services) > 0 and len(all_non_latency_services) > 1:
        # 如果cpu和memory存在多个服务，只保留在latency列表中出现的服务
        filtered_cpu = [item for item in cpu_list if item.split('.')[0] in service_services]
        filtered_memory = [item for item in memory_list if item.split('.')[0] in service_services]
        combined = filtered_cpu + filtered_memory + serveice_list + jvm_list
        # 记录筛选证据
        for item in combined:
            evidences_dict[item].append(
                f"因同时存在于latency列表和cpu/memory列表中，{item}被保留为候选根因"
            )
    else:
        # 否则直接合并所有列表
        combined = cpu_list + memory_list + serveice_list + jvm_list

    service_root_causes = {}  # 存储每个服务的最高优先级根因
    priority = {'memory': 4, 'cpu': 3, 'jvmChaos': 2, 'networkLatency': 1}  # 优先级映射

    for item in combined:
        # 解析服务名和根因类型
        parts = item.split('.')
        if len(parts) != 2:
            continue  # 跳过格式异常的项
        service, cause_type = parts[0], parts[1]

        # 仅处理已知类型
        if cause_type not in priority:
            continue

        # 更新当前服务的最高优先级根因
        if service not in service_root_causes:
            # 服务首次出现，直接记录
            service_root_causes[service] = (priority[cause_type], item)
        else:
            # 比较优先级，保留更高的
            current_prio, _ = service_root_causes[service]
            if priority[cause_type] > current_prio:
                service_root_causes[service] = (priority[cause_type], item)

    # 提取最终根因（只保留每个服务的最高优先级项）
    root_causes = [item for (_, item) in service_root_causes.values()]

    # 根据service出现频率筛选根因，只保留出现次数最多的service的根因
    if root_causes:
        # 筛选出频率最高的service对应的根因
        root_causes = [item for item in root_causes if item.split('.')[0] in fre]
        print(f"🎯 按频率筛选后的根因列表: {root_causes}")

    # 保留所有根因中优先级最高的根因
    priority_causes = ""
    current_prio = 0
    for item in root_causes:
        parts = item.split('.')
        if len(parts) != 2:
            continue  # 跳过格式异常的项
        service, cause_type = parts[0], parts[1]
        if priority[cause_type] > current_prio:
            current_prio = priority[cause_type]
            priority_causes = cause_type
    root_causes = [item for item in root_causes if item.split('.')[1] == priority_causes]
    print(f"🎯 按优先级筛选后的根因列表: {root_causes}")

    # 当存在多个延迟候选根因且不存在其他类型根因时，按照延迟上升幅度筛选Latency
    if len(cpu_list) == 0 and len(memory_list) == 0 and len(jvm_list) == 0 and len(root_causes) > 0:
        print("⚠️ 仅存在Latency异常，开始计算上升幅度筛选根因")
        root_causes, evidences_dict = get_only_anomaly(anomaly_list, root_causes, evidences_dict)

    # 处理 inventory 的情况
    if len(root_causes) > 0 and root_causes[0].split('.')[0] == "inventory":
        root_causes = ["inventory.jvmChaos"]
        evidences_dict["inventory.jvmChaos"].append(
            "inventory服务的根因被确定为jvmChaos"
        )

    # 处理 currency 的情况
    if len(root_causes) > 0 and root_causes[0] == "currency.cpu":
        flag = get_span_error(log_client, PROJECT_NAME, LOGSTORE_NAME, "currency", start_str.strip(), end_str.strip())
        if flag:
            print("🔍 获取 currency 服务网络异常数据...")
            root_causes = ["currency.networkLatency"]
            evidences_dict["currency.networkLatency"].append(
                "currency服务检测到网络异常，根因从CPU异常调整为网络延迟异常"
            )

    # 处理 frontend 和 checkout 的情况
    if latency == False and len(root_causes) > 0 and root_causes[0] in ['frontend.networkLatency', 'checkout.networkLatency']:
        services = []
        if root_causes[0].split('.')[0] == "frontend":
            services = ["ad", "recommendation", "checkout", "cart", "currency", "product-catalog"]
        elif root_causes[0].split('.')[0] == "checkout":
            services = ["product-catalog", "cart", "payment", "shipping", "email", "currency", "quote"]
        latency_candidates = []
        anomaly_list: List[Dict[str, Any]] = []
        for service in services:
            flag, before, target, after, _ = get_log(log_client, PROJECT_NAME, LOGSTORE_NAME, service,
                                                      start_str.strip(), end_str.strip(), False)
            if flag:
                print(f"🔍 获取 {service} 服务网络延迟数据...")
                latency_candidates.append(service + '.networkLatency')
                anomaly_list.append({
                    "service": service,
                    "before": before,
                    "target": target,
                    "after": after
                })
                evidences_dict[service + '.networkLatency'].append(
                    f"{service}服务检测到网络延迟异常，值为{target}"
                )
        # 如果只存在1-2个服务疑似上升，则不是checkout或frontend的问题
        if len(latency_candidates) < 3:
            root_causes, evidences_dict = get_only_anomaly(anomaly_list, latency_candidates, evidences_dict)

    if len(root_causes) == 0:
        print("⚠️ 根因列表为空，开始查询少见情况")
        target_service = "inventory"
        cpu_anomaly = analyze_cpu(normal_start, normal_end, target_service, False)
        memory_anomaly = analyze_memory(normal_start, normal_end, target_service, False)
        print(f"CPU异常: {cpu_anomaly}, Memory异常: {memory_anomaly}")
        if cpu_anomaly[0] or memory_anomaly[0]:
            root_causes.append(target_service + '.jvmChaos')
            evidences_dict[target_service + '.jvmChaos'].append(
                f"{target_service}服务在根因列表为空的情况下被检测到异常，被确定为jvmChaos问题"
            )
    print(f"🎯 筛选后的根因: {root_causes}")

    # 收集最终证据
    final_evidences = []
    for cause in root_causes:
        if cause in evidences_dict:
            final_evidences.extend(evidences_dict[cause])

    # 去重并保持顺序
    seen = set()
    final_evidences = [e for e in final_evidences if not (e in seen or seen.add(e))]

    return root_causes, root_cause_data, final_evidences

#处理灰色故障
def analyze_grey_failure(normal_start, normal_end, candidate_root_causes):
    anomaly_list: List[Dict[str, Any]] = []
    show = False
    cpu_list = []
    memory_list = []
    serveice_list = []
    combined = []
    root_cause_data = {}
    evidences_dict = defaultdict(list)  # 存储每个根因的证据
    start_str = normal_start.replace(tzinfo=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
    end_str = normal_end.replace(tzinfo=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')

    def process_one_service(service, normal_start, normal_end):
        result = {
            'service': service,
            'cpu_anomaly': False,
            'memory_anomaly': False
        }
        print(f"🎯 Limiting analysis to candidate service: {service}")

        # 4. 查询CPU数据
        print(f"🔍 查询 {service} 服务CPU数据...")
        cpu_anomaly, max_cpu, cpu_data = analyze_cpu(normal_start, normal_end, service, show)
        result['cpu_data'] = cpu_data
        result['max_cpu'] = max_cpu
        if cpu_anomaly and max_cpu > 30.0:
            # 记录CPU异常证据
            evidences_dict[service + '.cpu'].append(
                f"{service}的CPU使用率出现异常，最大值达到{max_cpu}%"
            )
            result['cpu_anomaly'] = True

        # 5. 查询Memory数据
        print(f"🔍 查询 {service} 服务Memory数据...")
        if service == "email":
            result['memory_anomaly'] = False
            result['memory_data'] = []
        else:
            memory_anomaly, max_memory, memory_data = analyze_memory(normal_start, normal_end, service, show)
            result['memory_data'] = memory_data
            result['max_memory'] = max_memory
            if memory_anomaly and max_memory > 15.0:
                # 记录内存异常证据
                evidences_dict[service + '.memory'].append(
                    f"{service}的内存使用率出现异常，最大值达到{max_memory}%"
                )
                result['memory_anomaly'] = True
        return result

    total_services = []
    for candidate in candidate_root_causes:
        if '.' in candidate and candidate.endswith('.cpu'):
            service = candidate.split('.')[0]
            if service[1] == '-' or service == "load-generator":
                continue
            total_services.append(service)

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_one_service, service, normal_start, normal_end) for service in total_services
        ]
        for future in as_completed(futures):
            result = future.result()
            service_name = result['service']
            if result['cpu_anomaly']:
                cpu_item = service_name + '.cpu'
                cpu_list.append(cpu_item)
                root_cause_data[cpu_item] = {
                    'cpu_data': result['cpu_data'],
                    'memory_data': result['memory_data'],
                }
            if result['memory_anomaly']:
                memory_item = service_name + '.memory'
                memory_list.append(memory_item)
                root_cause_data[memory_item] = {
                    'cpu_data': result['cpu_data'],
                    'memory_data': result['memory_data'],
                }

    print(f"🎯 cpu候选服务列表: {cpu_list}")
    print(f"🎯 memory候选服务列表: {memory_list}")
    if len(cpu_list) > 1:
        cpu_list = find_anomalies(cpu_list, root_cause_data)
    disk_list = []
    networkloss_list = []
    if len(cpu_list + memory_list) == 0:
        def process_one_service_ecs(service, normal_start, normal_end):
            result = {
                'service': service,
                'cpu_anomaly': False,
                'memory_anomaly': False,
                'disk_anomaly': False
            }
            print(f"🎯 Limiting analysis to candidate service: {service}")

            # 1. 查询CPU数据
            print(f"🔍 查询 {service} 服务CPU数据...")
            cpu_anomaly, max_cpu = analyze_ecs_cpu(normal_start, normal_end, service, show)
            if cpu_anomaly and max_cpu > 30.0:
                evidences_dict[service + '.cpu'].append(
                    f"{service}的CPU使用率出现异常，最大值达到{max_cpu}%"
                )
                result['cpu_anomaly'] = True

            # 2. 查询Memory数据
            print(f"🔍 查询 {service} 服务Memory数据...")
            # email服务内存长期存在OOM
            if service == "email":
                result['memory_anomaly'] = False
            else:
                memory_anomaly, max_memory = analyze_ecs_memory(normal_start, normal_end, service, show)
                if memory_anomaly and max_memory > 30.0:
                    evidences_dict[service + '.memory'].append(
                        f"{service}的内存使用率出现异常，最大值达到{max_memory}%"
                    )
                    result['memory_anomaly'] = True

            # 3. 查询Disk数据
            print(f"🔍 查询 {service} 服务Disk数据...")
            disk_anomaly, max_disk = analyze_ecs_disk(normal_start, normal_end, service, show)
            if disk_anomaly and max_disk > 30.0:
                disk_list.append(service + '.disk')
                evidences_dict[service + '.disk'].append(
                    f"{service}的磁盘使用率出现异常，最大值达到{max_disk}%"
                )
                result['disk_anomaly'] = True

            # 4. 获取网络异常
            anomaly = analyze_network(normal_start, normal_end, service, False)
            if anomaly >= 2:
                evidences_dict[service + '.networkLoss'].append(
                    f"{service}的网络丢包次数过多，存在网络异常"
                )
                networkloss_list.append(service + '.networkLoss')
                result['network_anomaly'] = True
            return result

        total_servies = []
        for candidate in candidate_root_causes:
            if '.' in candidate and candidate.endswith('.cpu'):
                service = candidate.split('.')[0]
                if service[1] != '-':
                    continue
                total_servies.append(service)

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(process_one_service_ecs, service, normal_start, normal_end) for service in total_servies
            ]
            for future in as_completed(futures):
                result = future.result()
                service_name = result['service']
                if result['cpu_anomaly']:
                    cpu_list.append(service_name + '.cpu')
                if result['memory_anomaly']:
                    memory_list.append(service_name + '.memory')

        root_causes = cpu_list + memory_list + disk_list + networkloss_list
        print(f"🎯 ecs cpu候选服务列表: {cpu_list}")
        print(f"🎯 ecs memory候选服务列表: {memory_list}")
        print(f"🎯 ecs disk候选服务列表: {disk_list}")
        print(f"🎯 ecs 网络异常服务列表: {networkloss_list}")

    service_root_causes = {}  # 存储每个服务的最高优先级根因
    priority = {'memory': 4, 'cpu': 3, 'disk': 2, 'networkLoss': 1}  # 优先级映射
    combined = cpu_list + memory_list + disk_list + networkloss_list
    for item in combined:
        # 解析服务名和根因类型
        parts = item.split('.')
        if len(parts) != 2:
            continue  # 跳过格式异常的项
        service, cause_type = parts[0], parts[1]

        # 仅处理已知类型
        if cause_type not in priority:
            continue

        # 更新当前服务的最高优先级根因
        if service not in service_root_causes:
            # 服务首次出现，直接记录
            service_root_causes[service] = (priority[cause_type], item)
        else:
            # 比较优先级，保留更高的
            current_prio, _ = service_root_causes[service]
            if priority[cause_type] > current_prio:
                service_root_causes[service] = (priority[cause_type], item)

    # 提取最终根因（只保留每个服务的最高优先级项）
    root_causes = [item for (_, item) in service_root_causes.values()]

    # 没有根因，则查询podKill的情况
    if len(root_causes) == 0:
        podKilled = []
        for candidate in candidate_root_causes:
            if '.' in candidate and candidate.endswith('.cpu'):
                service = candidate.split('.')[0]
                if service != 'checkout' and service != "frontend" and service != "product-catalog":
                    continue
                hostname_list = get_instance(log_client, PROJECT_NAME, LOGSTORE_NAME, service,
                                             start_str.strip(), end_str.strip())
                start = datetime.strptime(start_str.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone(timedelta(hours=8)))
                end = datetime.strptime(end_str.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone(timedelta(hours=8)))
                num = 0
                for hostname in hostname_list:
                    print(f"🔍 Found hostname {hostname}, processing...")
                    flag, _ = get_pod(start, end, hostname, True)
                    if not flag:
                        num += 1
                if 0 < num <= 2 and len(hostname_list) > 2:
                    print(f"✅ podKilled")
                    evidences_dict[service + '.podKiller'].append(
                        f"{service}服务的pod在检测时间段内被终止"
                    )
                    podKilled.append(service + '.podKiller')
        root_causes = podKilled

    if len(root_causes) == 0:
        print("⚠️ 根因列表为空，开始查询少见情况")
        target_service = "inventory"
        cpu_anomaly = analyze_cpu(normal_start, normal_end, target_service, False)
        memory_anomaly = analyze_memory(normal_start, normal_end, target_service, False)
        print(f"CPU异常: {cpu_anomaly}, Memory异常: {memory_anomaly}")
        if cpu_anomaly[0] or memory_anomaly[0]:
            evidences_dict[target_service + '.jvmChaos'].append(
                f"{target_service}服务在检测时间段内存在cpu和memory异常波动，可能是jvmchaos所导致的"
            )
            root_causes.append(target_service + '.jvmChaos')
    print(f"🎯 筛选后的根因列表: {root_causes}")

    if len(root_causes) == 0:
        flag, _, _, _, _ = get_log(log_client, PROJECT_NAME, LOGSTORE_NAME, "email", start_str.strip(), end_str.strip(), True, False)
        cpu_anomaly, _, _ = analyze_cpu(normal_start, normal_end, "email", show, False)
        if flag and cpu_anomaly:
            root_causes = ["email.memory"]
            evidences_dict["email.memory"].append(
                f"email服务在检测时间段内存在cpu异常下降，且延迟下降，可能是OOM所导致的"
            )

    if len(root_causes) == 0:
        print("⚠️ 根因列表依旧为空，查询延迟情况")
        def process_one_service(service, normal_start, normal_end, isMedian=True):
            result = {
                'service': service,
                'latency_anomaly': False,
                'anomaly_data': None,
                'latency_data': None  # 存储延迟数据
            }
            # 获取延迟数据
            print(f"🎯 Limiting analysis to candidate service: {service}")
            flag, before, target, after, duration_data = get_log(log_client, PROJECT_NAME, LOGSTORE_NAME, service,
                                                                 start_str.strip(),
                                                                 end_str.strip(), False)
            result['latency_data'] = duration_data
            if flag:
                evidences_dict[service + '.networkLatency'].append(
                    f"{service}服务检测到网络延迟异常，异常值为{target}，相比正常区间前半段({before})和后半段({after})存在明显上升！"
                )
                result['latency_anomaly'] = True
                result['anomaly_data'] = {
                    "service": service,
                    "before": before,
                    "target": target,
                    "after": after,
                }
            return result

        # 并行
        total_services = []
        for candidate in candidate_root_causes:
            if '.' in candidate and candidate.endswith('.cpu'):
                service = candidate.split('.')[0]
                if service[1] == '-' or service == "load-generator":
                    continue
                total_services.append(service)
        latency_candidates = []
        anomaly_list = []
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(process_one_service, service, normal_start, normal_end) for service in
                total_services
            ]
            for future in as_completed(futures):
                result = future.result()
                service_name = result['service']
                if result['latency_anomaly']:
                    latency_item = service_name + '.networkLatency'
                    latency_candidates.append(latency_item)
                    anomaly_list.append(result['anomaly_data'])

        root_causes, evidences_dict = get_only_anomaly(anomaly_list, latency_candidates, evidences_dict)
    print(f"🎯 筛选后的根因列表: {root_causes}")

    # 收集最终证据
    final_evidences = []
    for cause in root_causes:
        if cause in evidences_dict:
            final_evidences.extend(evidences_dict[cause])

    # 去重并保持顺序
    seen = set()
    final_evidences = [e for e in final_evidences if not (e in seen or seen.add(e))]
    return root_causes, root_cause_data, final_evidences

# 处理错误过多报警
def analyze_error_problem(normal_start, normal_end, candidate_root_causes):
    error_list = []
    anomaly_list: List[Dict[str, Any]] = []
    root_cause_data = {}
    evidences_dict = defaultdict(list)  # 存储每个根因的证据
    start_str = normal_start.replace(tzinfo=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
    end_str = normal_end.replace(tzinfo=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')

    def process_one_service(service, normal_start, normal_end):
        result = {
            'service': service,
            'error_anomaly': False,
            'anomaly_data': None
        }
        # 1. 查询报错数据
        print(f"🔍 查询 {service} 服务报错数据...")
        start_str = normal_start.replace(tzinfo=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        end_str = normal_end.replace(tzinfo=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        error_anomaly, _, target_error, _ = get_error(log_client, PROJECT_NAME, LOGSTORE_NAME, service,
                                                      start_str.strip(), end_str.strip())
        if error_anomaly and target_error > 2.0:
            result['error_anomaly'] = True
            result['anomaly_data'] = {
                "service": service,
                "error": target_error,
            }
            evidences_dict[service + '.Failure'].append(f"{service}服务在检测时间段内报错次数过多，报错次数为{target_error}")
        return result

    total_services = []
    for candidate in candidate_root_causes:
        if '.' in candidate and candidate.endswith('.cpu'):
            service = candidate.split('.')[0]
            if service[1] == '-' or service == "load-generator":
                continue
            total_services.append(service)

    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_one_service, service, normal_start, normal_end) for service in total_services
        ]
        for future in as_completed(futures):
            result = future.result()
            if result['error_anomaly']:
                error_list.append(result['service'] + '.Failure')
                anomaly_list.append(result['anomaly_data'])

    print(f"🎯 报错候选服务列表: {error_list}")
    # 从候选服务中筛选最下游应用
    # 1. 提取候选服务中的应用名
    candidate_services = [item.split('.')[0] for item in error_list]

    # 2. 构建候选服务之间的下游关系
    candidate_downstreams = {s: [] for s in candidate_services}
    for caller, callee in calls_relations:
        if caller in candidate_services and callee in candidate_services:
            candidate_downstreams[caller].append(callee)

    # 3. 筛选候选服务中没有下游的应用（最下游）
    most_downstream_in_candidates = [
        s for s in candidate_services
        if not candidate_downstreams[s]  # 下游列表为空
    ]

    # 4. 生成新的候选列表（只保留最下游应用）
    serveice_list = [
        item for item in error_list
        if item.split('.')[0] in most_downstream_in_candidates
    ]
    root_causes = serveice_list
    if len(root_causes) > 1:
        amplitude_dict = {}
        for anomaly in anomaly_list:
            service = anomaly['service']
            if service + '.Failure' not in root_causes:
                continue
            # 假设before、target为数值列表，取平均值计算
            try:
                target = anomaly['error']
                amplitude_dict[service] = target
            except Exception as e:
                print(f"❌ 获取target失败: {e}")

        if amplitude_dict:
            # 找到幅度最大的服务
            max_amplitude_service = max(amplitude_dict.items(), key=lambda x: x[1])[0]
            # 只保留该服务的根因
            root_causes = [item for item in root_causes if item.split('.')[0] == max_amplitude_service]
            print(f"🎯 按最大上升幅度筛选后的根因: {root_causes}")
    if len(root_causes) > 0 and root_causes[0].split('.')[0] == "inventory":
        print(f"🔍 查询 inventory 服务CPU数据...")
        cpu_anomaly, max_cpu, _ = analyze_cpu(normal_start, normal_end, "inventory", False)
        if cpu_anomaly:
            root_causes = ["inventory.jvmChaos"]
            evidences_dict["inventory" + '.jvmChaos'].append(
                f"inventory服务在检测时间段内错误过多且伴有CPU异常波动，可能是jvmChaos导致的")
        flag, _, _, _, _ = get_log(log_client, PROJECT_NAME, LOGSTORE_NAME, "inventory", start_str.strip(), end_str.strip(), True, False)
        if flag:
            root_causes = ["inventory.jvmChaos"]
            evidences_dict["inventory" + '.jvmChaos'].append(
                f"inventory服务在检测时间段内错误过多且大量请求延迟异常降低，可能是jvmChaos导致的")
    print(f"🎯 筛选后的根因列表: {root_causes}")

    # 收集最终证据
    final_evidences = []
    for cause in root_causes:
        if cause in evidences_dict:
            final_evidences.extend(evidences_dict[cause])

    # 去重并保持顺序
    seen = set()
    final_evidences = [e for e in final_evidences if not (e in seen or seen.add(e))]
    return root_causes, root_cause_data, final_evidences
