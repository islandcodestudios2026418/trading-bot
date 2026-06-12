FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY arb_monitor.py .
CMD ["python", "-u", "arb_monitor.py"]
