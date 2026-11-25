FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY connector.json app.py ./
ENTRYPOINT ["python", "-u", "/app/app.py"]
