import json
import os
import time
from datetime import datetime, timedelta, timezone

from aliyun.log import LogClient, GetLogsRequest
from matplotlib import pyplot as plt

# SLS configuration
PROJECT_NAME = "proj-xtrace-a46b97cfdc1332238f714864c014a1b-cn-qingdao"
LOGSTORE_NAME = "logstore-tracing"
REGION = "cn-qingdao"

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

def datetime_to_timestamp(time_str):
    # 解析时间字符串为datetime对象（默认本地时区，如需UTC可指定tzinfo）
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

    # 计算与epoch时间的差值（秒），再转换为毫秒
    timestamp_ms = int(dt.timestamp() * 1000)

    print(timestamp_ms)  # 输出：1758325449000
    return timestamp_ms

def dt_to_ms(dt):
    return int(dt.timestamp() * 1000)
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

def get_errorInfo(log_client, project, logstore, service, start, end):
    """获取指定时间段内特定节点上各hostname的平均duration"""
    start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    start = int(start_dt.timestamp()) * 1000000000
    end = int(end_dt.timestamp()) * 1000000000

    # 构建查询语句，筛选特定节点并按hostname分组
    query = f"""
    (serviceName : "{service}") AND statusCode>1
    | SELECT statusmessage as info FROM log group by info order by count(info) DESC LIMIT 0, 999 
    """
    print(query)
    request = GetLogsRequest(
        project=project,
        logstore=logstore,
        query=query,
        fromTime=start_dt.timestamp(),
        toTime=end_dt.timestamp()
    )
    response = log_client.get_logs(request)
    logs = response.get_logs()
    print(f"✅ 获取日志成功，共 {len(logs)} 条")
    print(logs[0].get_contents().get("info"))
    return logs[0].get_contents().get("info")

def get_span_error(log_client, project, logstore, service, start, end, isMedian=True):
    """获取指定时间段内特定节点上各hostname的平均duration"""
    start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    start_minus_5 = start_dt - timedelta(minutes=10)
    end_plus_5 = end_dt + timedelta(minutes=10)
    start_dt = start_dt - timedelta(minutes=1)
    end_dt = end_dt + timedelta(minutes=1)
    start_minus = int(start_minus_5.timestamp()) * 1000000000
    end_plus = int(end_plus_5.timestamp()) * 1000000000

    # 构建查询语句，筛选特定节点并按hostname分组
    query = f"""
    (statusCode : 2 or statusCode : 3) and spanName : "grpc.oteldemo.CurrencyService/GetSupportedCurrencies" and attributes.grpc.error_message : "14 UNAVAILABLE: read ECONNRESET"
    | SELECT count(statusCode) as statusCode, (startTime/1000000 -startTime/1000000 %(15000 * 4)) as date FROM log GROUP BY date LIMIT 0, 999 
    """

    request = GetLogsRequest(
        project=project,
        logstore=logstore,
        query=query,
        fromTime=start_minus_5.timestamp(),
        toTime=end_plus_5.timestamp()
    )
    response = log_client.get_logs(request)
    logs = response.get_logs()
    print(f"✅ 获取日志成功，共 {len(logs)} 条")

    # 核心改进：按time字段的时间顺序排序
    # 1. 先将日志转换为包含时间和值的字典列表
    log_list = []
    for log in logs:
        contents = log.get_contents()
        print(contents)
        time_stamp_str = contents.get("date")  # 毫秒时间戳字符串，如"1758326280000"
        avg_duration = contents.get("statusCode")

        if time_stamp_str and avg_duration:  # 过滤无效数据
            try:
                # 转换为整数型时间戳（确保排序准确性）
                time_stamp = int(time_stamp_str)
                log_list.append({
                    "time_ms": time_stamp,
                    # 同时转换为可读时间格式（用于x轴显示）
                    "time_str": datetime.fromtimestamp(time_stamp / 1000).replace(tzinfo=timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
                    "statusCode": float(avg_duration)
                })
            except ValueError:
                print(f"⚠️ 无效的时间戳格式: {time_stamp_str}，已跳过")
    # print(log_list)
    # 按时间戳数值排序（从小到大，即时间先后顺序）
    log_list.sort(key=lambda x: x["time_ms"])

    # 1. 定义三个时段的时间范围（毫秒级）
    # 前5分钟：[start_minus_5, start_dt)
    before_start_ms = dt_to_ms(start_minus_5)
    before_end_ms = dt_to_ms(start_dt)
    # 目标时段：[start_dt, end_dt)
    target_start_ms = dt_to_ms(start_dt)
    target_end_ms = dt_to_ms(end_dt)
    # 后5分钟：[end_dt, end_plus_5)
    after_start_ms = dt_to_ms(end_dt)
    after_end_ms = dt_to_ms(end_plus_5)

    # 2. 数据分组到三个时段
    before_data = []  # 前5分钟时延
    target_data = []  # 目标时段时延
    after_data = []  # 后5分钟时延

    for item in log_list:
        ts = item["time_ms"]
        duration = item["statusCode"]
        if before_start_ms <= ts < before_end_ms:
            before_data.append(duration)
        elif target_start_ms <= ts < target_end_ms:
            target_data.append(duration)
        elif after_start_ms <= ts < after_end_ms:
            after_data.append(duration)

    # 3. 计算各时段平均错误数量（使用中位数可减少异常值影响）
    def calc_statistic(data, is_median=True):
        if not data:
            return None
        if is_median:
            sorted_data = sorted(data)
            n = len(sorted_data)
            return sorted_data[n // 2] if n % 2 else (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        return sum(data) / len(data)

    before_stat = calc_statistic(before_data, isMedian)  # 前5分钟统计值
    target_stat = calc_statistic(target_data, isMedian)  # 目标时段统计值
    after_stat = calc_statistic(after_data, isMedian)  # 后5分钟统计值

    # 4. 输出统计结果
    print("\n=== 报错统计对比 ===")
    print(f"前5分钟（{start_minus_5.strftime('%H:%M:%S')}至{start_dt.strftime('%H:%M:%S')}）: "
          f"{before_stat:.2f}" if before_stat else "前5分钟无数据")
    print(f"目标时段（{start}至{end}）: "
          f"{target_stat:.2f}" if target_stat else "目标时段无数据")
    print(f"后5分钟（{end_dt.strftime('%H:%M:%S')}至{end_plus_5.strftime('%H:%M:%S')}）: "
          f"{after_stat:.2f}" if after_stat else "后5分钟无数据")

    # 5. 判断是否明显上升
    threshold = 1.5
    if target_stat and before_stat and after_stat:
        rise_ratio_before = (target_stat - before_stat) / before_stat * 100
        rise_ratio_after = (target_stat - after_stat) / after_stat * 100
        if target_stat > before_stat * threshold and target_stat > after_stat * threshold:
            print(
                f"\n⚠️ 目标时段报错相比前10分钟上升{rise_ratio_before:.1f}%，相比后10分钟上升{rise_ratio_after:.1f}%，超过{int((threshold - 1) * 100)}%，存在明显上升！")
            return True
        else:
            print(
                f"\n✅ 目标时段报错相比前10分钟上升{rise_ratio_before:.1f}%，相比后10分钟上升{rise_ratio_after:.1f}%，未超过{int((threshold - 1) * 100)}%，无明显上升。")
    elif target_stat and (not before_stat or not after_stat):
        print(f"\n⚠️ 存在异常报错，请检查日志。")
        return True
    else:
        print("\n⚠️ 数据不足，无法判断时延变化。")

    return False


def get_error(log_client, project, logstore, service, start, end, isMedian=True):
    """获取指定时间段内特定节点上各hostname的平均duration"""
    start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    # 计算时间差（分钟为单位，取整数）
    time_diff_minutes = int((end_dt - start_dt).total_seconds() / 60)
    start_minus_5 = start_dt - timedelta(minutes=10)
    end_plus_5 = end_dt + timedelta(minutes=10)
    start_dt = start_dt - timedelta(minutes=1)
    end_dt = end_dt + timedelta(minutes=1)
    start_minus = int(start_minus_5.timestamp()) * 1000000000
    end_plus = int(end_plus_5.timestamp()) * 1000000000

    # 构建查询语句，筛选特定节点并按hostname分组
    query = f"""
    ((serviceName : "{service}") AND startTime in [{start_minus} {end_plus})) AND statusCode>1
    | SELECT count(statusCode) as statusCode, (startTime/1000000 -startTime/1000000 %(15000 * 4)) as date FROM log GROUP BY date LIMIT 0, 999 
    """

    request = GetLogsRequest(
        project=project,
        logstore=logstore,
        query=query,
        fromTime=start_minus_5.timestamp(),
        toTime=end_plus_5.timestamp()
    )
    response = log_client.get_logs(request)
    logs = response.get_logs()
    print(f"✅ 获取日志成功，共 {len(logs)} 条")

    # 核心改进：按time字段的时间顺序排序
    # 1. 先将日志转换为包含时间和值的字典列表
    log_list = []
    for log in logs:
        contents = log.get_contents()
        #print(contents)
        time_stamp_str = contents.get("date")  # 毫秒时间戳字符串，如"1758326280000"
        avg_duration = contents.get("statusCode")

        if time_stamp_str and avg_duration:  # 过滤无效数据
            try:
                # 转换为整数型时间戳（确保排序准确性）
                time_stamp = int(time_stamp_str)
                log_list.append({
                    "time_ms": time_stamp,
                    # 同时转换为可读时间格式（用于x轴显示）
                    "time_str": datetime.fromtimestamp(time_stamp / 1000).replace(tzinfo=timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
                    "statusCode": float(avg_duration)
                })
            except ValueError:
                print(f"⚠️ 无效的时间戳格式: {time_stamp_str}，已跳过")
    #print(log_list)
    # 按时间戳数值排序（从小到大，即时间先后顺序）
    log_list.sort(key=lambda x: x["time_ms"])

    # 1. 定义三个时段的时间范围（毫秒级）
    # 前5分钟：[start_minus_5, start_dt)
    before_start_ms = dt_to_ms(start_minus_5)
    before_end_ms = dt_to_ms(start_dt)
    # 目标时段：[start_dt, end_dt)
    target_start_ms = dt_to_ms(start_dt)
    target_end_ms = dt_to_ms(end_dt)
    # 后5分钟：[end_dt, end_plus_5)
    after_start_ms = dt_to_ms(end_dt)
    after_end_ms = dt_to_ms(end_plus_5)

    # 2. 数据分组到三个时段
    before_data = []  # 前5分钟时延
    target_data = []  # 目标时段时延
    after_data = []  # 后5分钟时延

    for item in log_list:
        ts = item["time_ms"]
        duration = item["statusCode"]
        if before_start_ms <= ts < before_end_ms:
            before_data.append(duration)
        elif target_start_ms <= ts < target_end_ms:
            target_data.append(duration)
        elif after_start_ms <= ts < after_end_ms:
            after_data.append(duration)

    # 3. 计算各时段平均错误数量（使用中位数可减少异常值影响）
    def calc_statistic(data, is_median=True):
        if not data:
            return None
        # if is_median:
        #     sorted_data = sorted(data)
        #     n = len(sorted_data)
        #     return sorted_data[n // 2] if n % 2 else (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        return sum(data) / time_diff_minutes

    before_stat = calc_statistic(before_data, isMedian)  # 前5分钟统计值
    target_stat = calc_statistic(target_data, isMedian)  # 目标时段统计值
    after_stat = calc_statistic(after_data, isMedian)  # 后5分钟统计值

    # 4. 输出统计结果
    print("\n=== 报错统计对比 ===")
    print(f"前5分钟（{start_minus_5.strftime('%H:%M:%S')}至{start_dt.strftime('%H:%M:%S')}）: "
          f"{before_stat:.2f}" if before_stat else "前5分钟无数据")
    print(f"目标时段（{start}至{end}）: "
          f"{target_stat:.2f}" if target_stat else "目标时段无数据")
    print(f"后5分钟（{end_dt.strftime('%H:%M:%S')}至{end_plus_5.strftime('%H:%M:%S')}）: "
          f"{after_stat:.2f}" if after_stat else "后5分钟无数据")

    # 5. 判断是否明显上升（阈值可调整，这里设为50%）
    threshold = 1.5  # 超过前5分钟的1.5倍视为明显上升
    if target_stat and before_stat and after_stat:
        rise_ratio_before = (target_stat - before_stat) / before_stat * 100
        rise_ratio_after = (target_stat - after_stat) / after_stat * 100
        if target_stat > before_stat * threshold and target_stat > after_stat * threshold:
            print(f"\n⚠️ 目标时段报错相比前10分钟上升{rise_ratio_before:.1f}%，相比后10分钟上升{rise_ratio_after:.1f}%，超过{int((threshold - 1) * 100)}%，存在明显上升！")
            return True, before_stat, target_stat, after_stat
        else:
            print(f"\n✅ 目标时段报错相比前10分钟上升{rise_ratio_before:.1f}%，相比后10分钟上升{rise_ratio_after:.1f}%，未超过{int((threshold - 1) * 100)}%，无明显上升。")
    elif target_stat and (not before_stat or not after_stat):
        print(f"\n⚠️ 存在异常报错，请检查日志。")
        return True, before_stat, target_stat, after_stat
    else:
        print("\n⚠️ 数据不足，无法判断时延变化。")



    # # 6. 可视化（标记三个时段）
    # plt.figure(figsize=(12, 6))
    # x_dt = [datetime.strptime(item["time_str"], "%Y-%m-%d %H:%M:%S") for item in log_list]
    # y = [item["statusCode"] for item in log_list]
    # print(x_dt)
    # print(y)
    #
    # plt.plot(x_dt, y, marker='o', linestyle='-', color='b')
    # # 用阴影标记三个时段
    # plt.axvspan(start_minus_5, start_dt, color='lightgreen', alpha=0.3, label='前5分钟')
    # plt.axvspan(start_dt, end_dt, color='lightcoral', alpha=0.3, label='目标时段')
    # plt.axvspan(end_dt, end_plus_5, color='lightblue', alpha=0.3, label='后5分钟')
    #
    # # 显示图表
    # plt.show()
    return False, before_stat, target_stat, after_stat

if __name__ == "__main__":
    serveice_list = []
    problem_id = "040"
    input_data = read_input_data("../input.jsonl")
    for problem_data in input_data:
        if problem_data.get("problem_id") == problem_id:
            print(f"🔍 Found problem {problem_id}, processing...")
            problem_id = problem_data.get("problem_id", "unknown")
            time_range = problem_data.get("time_range", "")
            candidate_root_causes = problem_data.get("candidate_root_causes", [])
            alarm_rules = problem_data.get("alarm_rules", [])

            start_time, end_time = time_range.split(' ~ ')
            start_time = start_time.strip()
            end_time = end_time.strip()
            flag = get_error(log_client, PROJECT_NAME, LOGSTORE_NAME, "ad", start_time, end_time)
            # flag = get_span_error(log_client, PROJECT_NAME, LOGSTORE_NAME, "currency", start_time, end_time)
            # get_errorInfo(log_client, PROJECT_NAME, LOGSTORE_NAME, "cart", start_time, end_time)
    #         for candidate in candidate_root_causes:
    #             if '.' in candidate and candidate.endswith('.cpu'):
    #                 service = candidate.split('.')[0]
    #                 if service[1] == '-' or service == "image-provider" or service == "load-generator":
    #                     continue
    #                 print(f"🎯 Limiting analysis to candidate service: {service}")
    #                 flag = get_log(log_client, PROJECT_NAME, LOGSTORE_NAME, service, start_time, end_time)
    #                 if flag:
    #                     serveice_list.append(service)
    # print(serveice_list)