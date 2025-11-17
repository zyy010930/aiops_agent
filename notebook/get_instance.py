import json
import os
import time
from datetime import datetime, timedelta

from aliyun.log import LogClient, GetLogsRequest
from matplotlib import pyplot as plt
from get_entity import get_pod, get_pod_metrics

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

def get_instance(log_client, project, logstore, service, start, end):
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
    ((serviceName : "{service}") AND startTime in [{start_minus} {end_plus}))
    | SELECT hostname, count(*) as invoke FROM log GROUP BY hostname
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

    hostname_list = []
    for log in logs:
        contents = log.get_contents()
        hostname = contents.get("hostname")
        invoke = contents.get("invoke")
        # print(hostname, invoke)
        hostname_list.append(hostname)

    return hostname_list



if __name__ == "__main__":
    serveice_list = []
    problem_id = "056"
    input_data = read_input_data("../input.jsonl")
    podKilled = []
    for problem_data in input_data:
        if problem_data.get("problem_id") == problem_id:
        #if problem_data.get("alarm_rules")[0] == 'greyFailure':
            print(f"🔍 Found problem {problem_id}, processing...")
            problem_id = problem_data.get("problem_id", "unknown")
            time_range = problem_data.get("time_range", "")
            candidate_root_causes = problem_data.get("candidate_root_causes", [])
            alarm_rules = problem_data.get("alarm_rules", [])

            start_time, end_time = time_range.split(' ~ ')
            hostname_list = get_instance(log_client, PROJECT_NAME, LOGSTORE_NAME, "email", start_time.strip(), end_time.strip())
            start = datetime.strptime(start_time.strip(), "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime(end_time.strip(), "%Y-%m-%d %H:%M:%S")
            for hostname in hostname_list:
                print(f"🔍 Found hostname {hostname}, processing...")
                cpu = get_pod_metrics(start, end, hostname, True)

    #         for candidate in candidate_root_causes:
    #             if '.' in candidate and candidate.endswith('.cpu'):
    #                 service = candidate.split('.')[0]
    #                 # if service[1] == '-' or service == "load-generator":
    #                 #     continue
    #                 if service != 'checkout' and service != "frontend" and service != "product-catalog":
    #                     continue
    #                 hostname_list = get_instance(log_client, PROJECT_NAME, LOGSTORE_NAME, service, start_time.strip(), end_time.strip())
    #                 start = datetime.strptime(start_time.strip(), "%Y-%m-%d %H:%M:%S")
    #                 end = datetime.strptime(end_time.strip(), "%Y-%m-%d %H:%M:%S")
    #                 num = 0
    #                 for hostname in hostname_list:
    #                     print(f"🔍 Found hostname {hostname}, processing...")
    #                     flag, _ = get_pod(start, end, hostname, True)
    #                     if not flag:
    #                         num += 1
    #                 if 0 < num <= 2 and len(hostname_list) > 2:
    #                     print(f"✅ podKilled {problem_id}")
    #                     podKilled.append({problem_id, service})
    #
    # print(podKilled)
