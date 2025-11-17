FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY af_client.py etl_to_db.py ./

CMD ["python", "etl_to_db.py"]
