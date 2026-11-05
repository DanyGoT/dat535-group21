#!/usr/bin/env bash
# Run one full operating day under the VM's shared Spark lock and resource budget.
# Invoke in a transient systemd user service with MemoryMax=5G and CPUQuota=300%.
set -euo pipefail

day=${1:?Usage: run_week_validation.sh YYYY-MM-DD}
if [[ ! "$day" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  printf 'Invalid operating day: %s\n' "$day" >&2
  exit 2
fi
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
validation_root=/data/dat535/silver-week-validation
mkdir -p "$validation_root/logs" "$validation_root/events" "$validation_root/spark-scratch"

export DAT535_ENV=prod
export DAT535_LANDING_DIR=/data/dat535/landing
export DAT535_OUTPUT_DIR=/data/dat535/prod
export DAT535_SPARK_LOCAL_DIR="$validation_root/spark-scratch"
export SPARK_MASTER='local[2]'
export SPARK_DRIVER_MEMORY=2g

exec 9>/data/dat535/.spark.lock
flock -w 10800 9
awk '/^MemAvailable:/ {if ($2 < 5242880) {print "Less than 5 GiB available RAM; refusing to start"; exit 1}}' /proc/meminfo
available_kib=$(df -Pk /data | awk 'NR == 2 {print $4}')
if (( available_kib < 104857600 )); then
  printf 'Less than 100 GiB available disk; refusing to start\n' >&2
  exit 1
fi

printf 'Starting operating day %s at %s\n' "$day" "$(date -u +%FT%TZ)"
free -m
df -h / /data

common=(--master 'local[2]' --driver-memory 2g
  --conf spark.pyspark.python=/home/ubuntu/spark-env/bin/python
  --conf spark.pyspark.driver.python=/home/ubuntu/spark-env/bin/python
  --conf "spark.local.dir=$DAT535_SPARK_LOCAL_DIR"
  --conf spark.ui.enabled=false
  --conf spark.eventLog.enabled=true
  --conf "spark.eventLog.dir=file://$validation_root/events")

/usr/bin/time -v -o "$validation_root/logs/$day.silver.resources" \
  /opt/spark/bin/spark-submit "${common[@]}" "$script_dir/silver.py" --date "$day" \
  > "$validation_root/logs/$day.silver.log" 2>&1

/usr/bin/time -v -o "$validation_root/logs/$day.gold.resources" \
  /opt/spark/bin/spark-submit "${common[@]}" "$script_dir/gold_scorecard.py" --date "$day" \
  > "$validation_root/logs/$day.gold.log" 2>&1

printf 'Finished operating day %s at %s\n' "$day" "$(date -u +%FT%TZ)"
free -m
df -h / /data
