#!/usr/bin/env bash
# Đóng gói package `etl` và submit job lên Spark standalone cluster.
# Dùng: /opt/submit.sh [--run-date YYYY-MM-DD] [--skip-extract]
set -euo pipefail

APP_DIR=/opt/spark-apps
cd "$APP_DIR"
rm -f /tmp/etl.zip
python3 -m zipfile -c /tmp/etl.zip etl/

exec spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --name oltp-to-olap \
  --py-files /tmp/etl.zip \
  "$APP_DIR/main.py" "$@"
