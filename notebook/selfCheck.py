import json
from typing import Dict, Set, List, Any, Union


def load_jsonl_data(file_path: str) -> Dict[str, Set[str]]:
    """
    加载JSONL文件并转换为{problem_id: root_causes集合}的字典
    保留空root_causes（转为空集合）
    """
    data_dict = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                problem_id = entry.get('problem_id')
                root_causes = entry.get('root_causes', [])

                if problem_id is None:
                    print(f"警告：文件 {file_path} 第 {line_num} 行缺少problem_id，已跳过")
                    continue

                # 保留空root_causes（转为空集合）
                data_dict[problem_id] = set(root_causes)

            except json.JSONDecodeError:
                print(f"警告：文件 {file_path} 第 {line_num} 行格式错误，已跳过")
            except Exception as e:
                print(f"警告：文件 {file_path} 第 {line_num} 行处理出错：{e}，已跳过")

    return data_dict


def load_problem_data(file_path: str) -> Dict[str, List[str]]:
    """
    加载问题文件（JSONL），返回{problem_id: alarm_rules}的字典
    """
    problem_data = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                problem_id = entry.get('problem_id')
                alarm_rules = entry.get('alarm_rules', [])
                if problem_id is None:
                    print(f"警告：问题文件 {file_path} 第 {line_num} 行缺少problem_id，已跳过")
                    continue
                problem_data[problem_id] = alarm_rules
            except json.JSONDecodeError:
                print(f"警告：问题文件 {file_path} 第 {line_num} 行格式错误，已跳过")
            except Exception as e:
                print(f"警告：问题文件 {file_path} 第 {line_num} 行处理出错：{e}，已跳过")
    return problem_data


def compare_root_causes(correct_file: str, test_file: str, problem_file: str) -> None:
    """
    比较两个JSONL文件的root_causes，按alarm_rules分组输出差异，按problem_id排序
    包含root_causes为空的差异情况
    """
    # 加载数据
    correct_data = load_jsonl_data(correct_file)
    test_data = load_jsonl_data(test_file)
    problem_data = load_problem_data(problem_file)

    # 所有需要比较的problem_id（取并集，单方存在也算差异）
    all_pids = set(correct_data.keys()).union(set(test_data.keys()))
    if not all_pids:
        print("没有找到任何problem_id，无法进行比较")
        return

    # 收集所有差异条目
    diff_entries: List[Dict[str, Any]] = []
    for pid in all_pids:
        correct_rc = correct_data.get(pid, set())
        test_rc = test_data.get(pid, set())

        # 只要不相等就视为差异（包括空集合的情况）
        if correct_rc != test_rc:
            # 获取对应的alarm_rules（不存在则标记为缺失）
            alarm_rules = problem_data.get(pid, None)
            diff_entries.append({
                "problem_id": pid,
                "correct": list(correct_rc),
                "test": list(test_rc),
                "alarm_rules": alarm_rules
            })

    if not diff_entries:
        print("所有problem_id的root_causes完全相同")
        return

    # 按alarm_rules分组（将列表转为元组作为键，处理缺失情况）
    grouped: Dict[Union[tuple, str], List[Dict[str, Any]]] = {}
    for entry in diff_entries:
        ar = entry["alarm_rules"]
        # 用元组作为键（列表不可哈希），缺失的alarm_rules用特殊标记
        key = tuple(ar) if ar is not None else "<alarm_rules缺失>"
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(entry)

    # 每个分组内按problem_id数值从小到大排序
    for key in grouped:
        grouped[key].sort(key=lambda x: int(x["problem_id"]))  # 按数字排序

    # 输出结果
    print(f"共发现 {len(diff_entries)} 个root_causes不一致的条目，按alarm_rules分组如下：\n")
    for group_idx, (ar_key, entries) in enumerate(sorted(grouped.items(), key=lambda x: str(x[0])), 1):
        # 显示分组的alarm_rules
        ar_display = list(ar_key) if ar_key != "<alarm_rules缺失>" else ar_key
        print(f"=== 分组 {group_idx}（alarm_rules: {ar_display}）共 {len(entries)} 条 ===")

        for entry in entries:
            print(f"problem_id: {entry['problem_id']}")
            print(f"  答案文件root_causes: {entry['correct']}")
            print(f"  待测文件root_causes: {entry['test']}\n")


if __name__ == "__main__":
    # 请替换为实际的文件路径
    CORRECT_FILE_PATH = "C://Users//17556//Desktop//aliyun//test11-14.jsonl"
    TEST_FILE_PATH = "output_linux.jsonl"
    PROBLEM_FILE_PATH = "C://Users//17556//Desktop//tianchi-2025-basic//B榜题目.jsonl"  # 问题文件路径

    compare_root_causes(CORRECT_FILE_PATH, TEST_FILE_PATH, PROBLEM_FILE_PATH)