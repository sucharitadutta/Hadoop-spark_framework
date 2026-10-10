#!/bin/sh
# shell_wrapper.sh -- run ONE HQL job from etl_config.ini with spark-submit
# Usage : sh shell_wrapper.sh <section_name> [run_date]
# Test  : DRY_RUN=1 sh shell_wrapper.sh <section_name>     (prints the command, runs nothing)

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CONFIG_FILE=${CONFIG_FILE:-$SCRIPT_DIR/etl_config.ini}
SECTION=$1
RUN_DATE=${2:-$(date +%Y-%m-%d)}
LOG_FILE=""

# ---- logger: prints to the screen and, once LOG_FILE is set, to the log file ----
logger() {
    level=$1; shift
    msg="$(date '+%Y-%m-%d %H:%M:%S') [$level] [$SECTION] $*"
    echo "$msg"
    [ -n "$LOG_FILE" ] && echo "$msg" >> "$LOG_FILE"
    return 0
}

# ---- ini reader: get_ini <key> [default]  ->  job value, else [global] value, else default ----
read_ini() {
    awk -F= -v sec="$1" -v key="$2" '
        /^[ \t]*[;#]/ { next }
        /^[ \t]*\[/   { s=$0; sub(/^[ \t]*\[/,"",s); sub(/\].*$/,"",s); cur=s; next }
        cur==sec { k=$1; gsub(/[ \t\r]/,"",k)
                   if (k==key) { v=substr($0,index($0,"=")+1); sub(/[ \t]+;.*$/,"",v)
                                 gsub(/^[ \t]+|[ \t\r]+$/,"",v); print v; exit } }' "$CONFIG_FILE"
}
get_ini() {
    v=$(read_ini "$SECTION" "$1")
    [ -n "$v" ] || v=$(read_ini global "$1")
    echo "${v:-$2}"
}

# ---- 1. checks ----
[ -n "$SECTION" ]      || { echo "Usage: sh $0 <section_name> [run_date]"; exit 1; }
[ -f "$CONFIG_FILE" ]  || { logger ERROR "config file not found: $CONFIG_FILE"; exit 2; }
grep -q "^\[$SECTION\]" "$CONFIG_FILE" || { logger ERROR "section [$SECTION] not found in $CONFIG_FILE"; exit 2; }

HQL_FILE=$(get_ini hql_base_dir "$SCRIPT_DIR/hql")/$(get_ini hql_path)
SRC_DB=$(get_ini source_db);   SRC_TABLE=$(get_ini source_table)
TGT_DB=$(get_ini target_db);   TGT_TABLE=$(get_ini target_table)
LOG_DIR=$(get_ini log_dir "$SCRIPT_DIR/logs")

mkdir -p "$LOG_DIR" && LOG_FILE="$LOG_DIR/${SECTION}_$(date +%Y%m%d_%H%M%S).log"
[ -f "$HQL_FILE" ]     || { logger ERROR "HQL file not found: $HQL_FILE"; exit 5; }
[ -n "$SRC_TABLE" ] && [ -n "$TGT_TABLE" ] || { logger ERROR "source_table and target_table are required"; exit 2; }

# ---- 2. spark-submit (runs the HQL file through Spark's SQL CLI driver, same as spark-sql -f) ----
set -- "$(get_ini spark_submit_bin spark-submit)" \
    --master yarn --deploy-mode client --name "etl_${SECTION}_${RUN_DATE}" \
    --queue "$(get_ini yarn_queue default)" \
    --driver-memory "$(get_ini driver_memory 2g)" \
    --executor-memory "$(get_ini executor_memory 4g)" \
    --executor-cores "$(get_ini executor_cores 2)" \
    --num-executors "$(get_ini num_executors 4)" \
    --class org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver spark-internal \
    --hivevar "run_date=$RUN_DATE" \
    --hivevar "src_db=$SRC_DB" --hivevar "src_table=$SRC_TABLE" \
    --hivevar "tgt_db=$TGT_DB" --hivevar "tgt_table=$TGT_TABLE" \
    -f "$HQL_FILE"

logger INFO "start: $SRC_DB.$SRC_TABLE -> $TGT_DB.$TGT_TABLE  run_date=$RUN_DATE  hql=$HQL_FILE"
if [ "${DRY_RUN:-0}" = 1 ]; then
    logger INFO "DRY RUN, command: $*"
    exit 0
fi

# ---- 3. run, log, return the exit code ----
"$@" >> "$LOG_FILE" 2>&1
rc=$?
if [ $rc -eq 0 ]; then
    logger INFO "SUCCESS (log: $LOG_FILE)"
else
    logger ERROR "FAILED with exit code $rc (log: $LOG_FILE)"
    tail -20 "$LOG_FILE"
fi
exit $rc
