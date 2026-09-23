#!/bin/sh
set -eu

until curl -sf "http://${FLINK_JOBMANAGER_HOST}:8081/overview" > /dev/null; do
  echo "waiting for jobmanager at ${FLINK_JOBMANAGER_HOST}:8081..."
  sleep 3
done

flink run -d -m "${FLINK_JOBMANAGER_HOST}:8081" /opt/job/orders-processing-job.jar
