#!/usr/bin/env bash
set -e

# создаём cron-файл с расписанием из переменной окружения
CRON_FILE=/etc/cron.d/integration-cron

# если переменная не задана – используем дефолт
: "${CRON_SCHEDULE:=*/5 * * * *}"

echo "Using CRON schedule: ${CRON_SCHEDULE}"

# перенаправляем вывод скрипта в лог
echo "${CRON_SCHEDULE} root python /app/integration.py >> /var/log/cron.log 2>&1" > "$CRON_FILE"

chmod 0644 "$CRON_FILE"

# требуется для cron
touch /var/log/cron.log

# запускаем cron в foreground, чтобы контейнер не завершился
cron && tail -f /var/log/cron.log
