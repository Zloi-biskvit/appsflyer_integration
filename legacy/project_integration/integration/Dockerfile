FROM python:3.12-slim

# Установка cron
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Зависимости Python
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Твой код
COPY integration.py .
COPY docker-cron-entrypoint.sh /docker-cron-entrypoint.sh
RUN chmod +x /docker-cron-entrypoint.sh

# Значение по умолчанию – раз в 5 минут
ENV CRON_SCHEDULE="*/5 * * * *"

CMD ["/docker-cron-entrypoint.sh"]
