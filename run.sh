#!/bin/bash
set -e

# 检查是否提供了输入输出文件参数
INPUT_FILE=${1:-"input.jsonl"}
OUTPUT_FILE=${2:-"output.jsonl"}
TIMEOUT=${3:-300}

echo "开始运行故障根因分析程序..."
echo "输入文件: $INPUT_FILE"
echo "输出文件: $OUTPUT_FILE"
echo "超时时间: $TIMEOUT秒"

# 运行主程序
python notebook/main.py \
    --input "$INPUT_FILE" \
    --output "$OUTPUT_FILE" \
    --timeout "$TIMEOUT"

echo "程序运行完成，结果已保存到$OUTPUT_FILE"