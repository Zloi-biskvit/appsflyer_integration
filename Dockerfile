version: "3.8"

services:
  prometheus:
    image: prom/prometheus:v2.53.0
    container_name: prometheus
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    command:
      - "--storage.tsdb.path=/prometheus"
      - "--storage.tsdb.retention.time=15d"
      - "--config.file=/etc/prometheus/prometheus.yml"
    ports: ["9090:9090"]
    restart: unless-stopped

  alertmanager:
    image: prom/alertmanager:v0.27.0
    container_name: alertmanager
    volumes:
      - ./alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
    ports: ["9093:9093"]
    restart: unless-stopped

  grafana:
    image: grafana/grafana:10.4.5
    container_name: grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    ports: ["3000:3000"]
    volumes:
      - grafana_data:/var/lib/grafana
    restart: unless-stopped

  node_exporter:
    image: prom/node-exporter:v1.8.2
    container_name: node_exporter
    network_mode: host
    pid: host
    restart: unless-stopped
    command:
      - '--path.rootfs=/host'
    volumes:
      - '/:/host:ro,rslave'

  postgres_exporter:
    image: prometheuscommunity/postgres-exporter:v0.15.0
    container_name: postgres_exporter
    environment:
      - DATA_SOURCE_NAME=postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=disable
    ports: ["9187:9187"]
    restart: unless-stopped

volumes:
  prometheus_data:
  grafana_data:
