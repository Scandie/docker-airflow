#!/usr/bin/env bash

TRY_LOOP="20"

: "${REDIS_HOST:="redis"}"
: "${REDIS_PORT:="6379"}"
: "${REDIS_PASSWORD:=""}"

: "${POSTGRES_HOST:="postgres"}"
: "${POSTGRES_PORT:="5432"}"
: "${POSTGRES_USER:="airflow"}"
: "${POSTGRES_PASSWORD:="airflow"}"
: "${POSTGRES_DB:="airflow"}"

# Default variables values for Airflow user
: "${AIRFLOW_USERNAME:="username"}"
: "${AIRFLOW_PASSWORD:="password"}"
: "${AIRFLOW_EMAIL:="username@mail.com"}"

# Max count of concurrent task per single worker
: "${MAX_ACTIVE_TASKS_PER_WORKER:="16"}"
# Max count of concurrent task per single dag
: "${MAX_ACTIVE_RUNS_PER_DAG:="4"}"
# Max count of parallel threads to schedule dags
: "${MAX_THREADS:="2"}"


# Defaults and back-compat
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"

# Variable for custom logging config that located in $PYTHONPATH, not used if null
: "${AIRFLOW__CORE__LOGGING_CONFIG_CLASS:="${LOG_CONF:=""}"}"

export \
  AIRFLOW__CELERY__BROKER_URL \
  AIRFLOW__CELERY__RESULT_BACKEND \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  AIRFLOW__CORE__SQL_ALCHEMY_CONN \
  AIRFLOW__CORE__LOGGING_CONFIG_CLASS \
  AIRFLOW__WEBSERVER__AUTHENTICATE \
  AIRFLOW__CORE__DAG_CONCURRENCY \
  AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG \
  AIRFLOW__SCHEDULER__MAX_THREADS \


if [[ -z "$AIRFLOW__CORE__DAG_CONCURRENCY" && -n "$MAX_ACTIVE_TASKS_PER_WORKER" ]]; then
    AIRFLOW__CORE__DAG_CONCURRENCY=${MAX_ACTIVE_TASKS_PER_WORKER}
fi

if [[ -z "$AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG" && -n "$MAX_ACTIVE_RUNS_PER_DAG" ]]; then
    AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=${MAX_ACTIVE_RUNS_PER_DAG}
fi

if [[ -z "$AIRFLOW__SCHEDULER__MAX_THREADS" && -n "$MAX_THREADS" ]]; then
    AIRFLOW__SCHEDULER__MAX_THREADS=${MAX_THREADS}
fi

# Load DAGs examples (default: Yes)
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]
then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi

# Using airflow auth (default: No)
if [[ -z "$AIRFLOW__WEBSERVER__AUTHENTICATE" && "${AUTH:=n}" == y ]]
then
  AIRFLOW__WEBSERVER__AUTHENTICATE=True
fi

# Install custom python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

if [ -n "$REDIS_PASSWORD" ]; then
    REDIS_PREFIX=:${REDIS_PASSWORD}@
else
    REDIS_PREFIX=
fi

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

if [ "$AIRFLOW__CORE__EXECUTOR" != "SequentialExecutor" ]; then
  AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
  AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
  wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
fi

if [ "$AIRFLOW__CORE__EXECUTOR" = "CeleryExecutor" ]; then
  AIRFLOW__CELERY__BROKER_URL="redis://$REDIS_PREFIX$REDIS_HOST:$REDIS_PORT/1"
  wait_for_port "Redis" "$REDIS_HOST" "$REDIS_PORT"
fi

case "$1" in
  webserver)
    airflow initdb

    # Using airflow auth (default: No)
    if [[ "$AIRFLOW__WEBSERVER__AUTHENTICATE" == True ]]
    then
      python scripts/create_user.py \
      -u ${AIRFLOW_USERNAME} \
      -p  ${AIRFLOW_PASSWORD} \
      -e ${AIRFLOW_EMAIL}
    fi

    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]; then
      # With the "Local" executor it should all run in one container.
      airflow scheduler &
    fi
    exec airflow webserver
    ;;
  worker|scheduler)
    # To give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  resetdb)
    exec airflow "$@" -y
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
