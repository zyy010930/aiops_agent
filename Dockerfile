# 基础镜像（选择合适的 Python 版本，如 3.9）
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制项目文件到容器中
COPY . /app

# 安装依赖（使用国内源加速，可选）
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# （可选）设置环境变量（如阿里云凭证，建议运行时通过 -e 传入，而非硬编码）
# ENV ALIBABA_CLOUD_ACCESS_KEY_ID=your_id
# ENV ALIBABA_CLOUD_ACCESS_KEY_SECRET=your_secret

# （可选）指定容器启动命令（根据实际入口脚本调整）
# 例如：运行测试用例
CMD ["python", "notebook/main.py"]