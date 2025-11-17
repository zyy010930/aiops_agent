import argparse
import json
from datetime import datetime, timezone, timedelta

from get_log import read_input_data
from parallel_agent import analyze_latency_problem, analyze_grey_failure, analyze_error_problem

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='故障根因分析程序')
    parser.add_argument('--input', default='input.jsonl', help='输入JSONL文件路径')
    parser.add_argument('--output', default='output.jsonl', help='输出JSONL文件路径')
    parser.add_argument('--timeout', type=int, default=300, help='单题最大处理时长(秒)')
    args = parser.parse_args()

    output_results = []
    input_data = read_input_data(args.input)
    for problem_data in input_data:
        problem_id = problem_data.get("problem_id", "unknown")
        time_range = problem_data.get("time_range", "")
        candidate_root_causes = problem_data.get("candidate_root_causes", [])
        alarm_rules = problem_data.get("alarm_rules", [])
        root_causes = []
        evidences_data = []

        start_str, end_str = time_range.split(' ~ ')
        normal_start = datetime.strptime(start_str.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone(timedelta(hours=8)))
        normal_end = datetime.strptime(end_str.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone(timedelta(hours=8)))

        # if problem_data.get("problem_id") != "050":
        #     continue

        if problem_data.get("alarm_rules")[0] == 'frontend_avg_rt' or problem_data.get("alarm_rules")[
            0] == 'service_avg_rt':
            root_causes, root_cause_data, evidences_data = analyze_latency_problem(normal_start, normal_end, candidate_root_causes)
        elif problem_data.get("alarm_rules")[0] == 'greyFailure':
            root_causes, root_cause_data, evidences_data = analyze_grey_failure(normal_start, normal_end, candidate_root_causes)
        elif problem_data.get("alarm_rules")[0] == 'overall_error_count':
            root_causes, root_cause_data, evidences_data = analyze_error_problem(normal_start, normal_end, candidate_root_causes)
        else:
            print(f"❌ 未知告警规则: {problem_data.get('alarm_rules')[0]}")
            continue

        # if len(root_causes) > 1:
        #     print("开始使用大模型进行分析")
        #     root_causes = [call_bailian_model(root_causes, root_cause_data)]
        #     print(f"🎯 根因列表: {root_causes}")

        # 添加到输出结果
        output_results.append({
            "problem_id": problem_id,
            "root_causes": root_causes,
            #"evidences": evidences_data
        })

    # 写入JSONL文件
    output_file_path = args.output
    with open(output_file_path, 'w', encoding='utf-8') as f:
        for result in output_results:
            f.write(json.dumps(result, ensure_ascii=False) + '\n')
    print(f"✅ 结果已写入 {output_file_path}")