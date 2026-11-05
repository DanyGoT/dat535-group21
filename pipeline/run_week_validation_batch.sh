#!/usr/bin/env bash
# Continue the requested week only after the clean full-day pilot has passed.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
validation_root=/data/dat535/silver-week-validation
prod_root=/data/dat535/prod
mkdir -p "$validation_root/logs" "$validation_root/checkpoints"

pilot_unit=dat535-silver-validation-retry-20261029-20261105.service
days=(2026-10-26 2026-10-27 2026-10-28 2026-10-29 2026-10-30 2026-10-31 2026-11-01)
printf 'Waiting for the pilot to finish at %s\n' "$(date -u +%FT%TZ)"
while [[ "$(systemctl --user show "$pilot_unit" --property=SubState --value)" == running ]]; do
  sleep 30
done

complete_day() {
  local day=$1
  local silver="$prod_root/silver/_runs/operating_date=$day/latest.json"
  local gold="$prod_root/gold/_runs/operating_date=$day/latest.json"
  [[ -f "$silver" && -f "$gold" ]] && jq -se '
    length == 2 and all(.[]; .status == "complete" and .ready == true)
    and .[1].silver_run_id == .[0].run_id' "$silver" "$gold" >/dev/null
}

if ! complete_day 2026-10-29; then
  printf 'The full-day pilot has not passed; refusing to queue the week.\n' >&2
  exit 1
fi

refresh_frontend() {
  local day
  local -a inputs=()
  for day in "${days[@]}"; do
    if complete_day "$day"; then
      inputs+=(--csv "$prod_root/gold/line_scorecard/operating_date=$day/_preview/scorecard.csv"
        --run-record "$prod_root/gold/_runs/operating_date=$day/latest.json")
    fi
  done
  /home/ubuntu/spark-env/bin/python "$script_dir/../examples/gold-scorecard/build.py" \
    "${inputs[@]}" --output-dir "$validation_root/frontend"
}

# A local static bundle follows every completed day. Hosted publication remains
# a separate Sites operation; no credentials are stored on this VM.
refresh_frontend

# Rebuild candidates separately. The original partition stays readable until its
# replacement is complete and its Bronze quality record has passed the checks.
backfill() (
  day=$1
  feed=$2
  dataset=$3
  checkpoint="$validation_root/checkpoints/$dataset-$day.complete"
  if [[ -f "$checkpoint" ]]; then return; fi
  exec 9>/data/dat535/.spark.lock
  flock -w 10800 9
  awk '/^MemAvailable:/ {if ($2 < 5242880) {print "Less than 5 GiB RAM available"; exit 1}}' /proc/meminfo
  available_kib=$(df -Pk /data | awk 'NR == 2 {print $4}')
  if (( available_kib < 104857600 )); then
    printf 'Less than 100 GiB disk available\n' >&2
    exit 1
  fi
  candidate="$validation_root/backfill/$dataset-$day"
  export DAT535_ENV=prod DAT535_LANDING_DIR=/data/dat535/landing
  export DAT535_OUTPUT_DIR="$candidate" SPARK_DRIVER_MEMORY=2g
  printf 'Backfilling %s %s at %s\n' "$dataset" "$day" "$(date -u +%FT%TZ)"
  /usr/bin/time -v -o "$validation_root/logs/$day.$feed.bronze.resources" \
    /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 2g \
    --conf spark.pyspark.python=/home/ubuntu/spark-env/bin/python \
    --conf spark.pyspark.driver.python=/home/ubuntu/spark-env/bin/python \
    --conf "spark.local.dir=$validation_root/spark-scratch" --conf spark.ui.enabled=false \
    "$script_dir/bronze.py" --date "$day" --feeds "$feed" \
    > "$validation_root/logs/$day.$feed.bronze.log" 2>&1

  source_partition="$candidate/bronze/$dataset/date=$day"
  test -f "$source_partition/_SUCCESS"
  records=("$candidate/bronze/_runs/date=$day/"*.json)
  jq -se --arg day "$day" --arg feed "$feed" '
    map(select(.date == $day)) | max_by(.finished_at) | .feeds[$feed]
    | .rows > 0 and (.bad_rows | length) == 0
      and (.missing_hours | length) == 0 and (.open_hours | length) == 0' \
    "${records[@]}" >/dev/null

  target="$prod_root/bronze/$dataset/date=$day"
  backup="$validation_root/bronze-backups/$dataset/date=$day"
  test ! -e "$backup"
  mkdir -p "$(dirname "$target")" "$(dirname "$backup")"
  if [[ -e "$target" ]]; then mv -- "$target" "$backup"; fi
  if ! mv -- "$source_partition" "$target"; then
    if [[ -e "$backup" ]]; then mv -- "$backup" "$target"; fi
    exit 1
  fi
  mkdir -p "$prod_root/bronze/_runs/date=$day"
  cp -- "${records[@]}" "$prod_root/bronze/_runs/date=$day/"
  printf 'Validated and installed %s %s at %s\n' "$dataset" "$day" "$(date -u +%FT%TZ)" > "$checkpoint"
)

backfill 2026-10-25 et siri-et
backfill 2026-10-26 et siri-et
backfill 2026-10-27 et siri-et
backfill 2026-10-26 vehicles vehicles
backfill 2026-10-27 vehicles vehicles

# Do not start another multi-hour historical job across the regular 03:00 UTC
# Bronze slot. This wait holds no Spark lock. Once that night's Entur and DATEX
# run records exist, historical processing can resume; 06:00 ends the reservation.
wait_for_nightly_slot() {
  local hour day cutoff announced=0
  local -a nightly_records
  while true; do
    hour=$(date -u +%H)
    case "$hour" in
      21|22|23|00|01|02) ;;
      03|04|05)
        day=$(date -u -d yesterday +%F)
        cutoff="$(date -u +%F)T03:00:00"
        nightly_records=("$prod_root/bronze/_runs/date=$day/"*.json)
        if [[ -f "${nightly_records[0]}" ]] && jq -se --arg day "$day" --arg cutoff "$cutoff" '
          map(select(.date == $day and .finished_at >= $cutoff))
          | any(.[]; .feeds.et != null and .feeds.vehicles != null)
            and any(.[]; .feeds.situation != null)' "${nightly_records[@]}" >/dev/null; then
          return
        fi
        ;;
      *) return ;;
    esac
    if (( announced == 0 )); then
      printf 'Reserving the overnight Bronze slot before starting another historical day (%s).\n' "$(date -u +%FT%TZ)"
      announced=1
    fi
    sleep 60
  done
}

for day in "${days[@]}"; do
  if complete_day "$day"; then
    printf 'Already complete: %s\n' "$day"
    continue
  fi
  wait_for_nightly_slot
  bash "$script_dir/run_week_validation.sh" "$day"
  refresh_frontend
done

records=()
for day in "${days[@]}"; do
  complete_day "$day"
  records+=("$prod_root/gold/_runs/operating_date=$day/latest.json")
done
jq -s '{start:"2026-10-26",end:"2026-11-01",status:"complete",ready:true,days:.}' \
  "${records[@]}" > "$validation_root/completed-week.json"
printf 'All seven operating days are complete.\n'
