#!/bin/sh
set -eu
envsubst < /etc/jmx-exporter/config.yml.template > /etc/jmx-exporter/config.yml
exec java -jar /opt/jmx_prometheus_httpserver.jar 5556 /etc/jmx-exporter/config.yml
