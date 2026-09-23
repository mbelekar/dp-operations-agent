# Scrapes a single Kafka broker's JMX and re-exposes only the two metrics
# LiveKafkaGateway.broker_jmx_metrics needs (IsrShrinksPerSec/IsrExpandsPerSec),
# with a static broker= label set per instance via BROKER_ID.
FROM eclipse-temurin:11-jre-jammy

ARG JMX_EXPORTER_VERSION=1.0.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends gettext-base curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /opt/jmx_prometheus_httpserver.jar \
       "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_httpserver/${JMX_EXPORTER_VERSION}/jmx_prometheus_httpserver-${JMX_EXPORTER_VERSION}.jar"

COPY kafka-broker.yml.template /etc/jmx-exporter/config.yml.template
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5556
ENTRYPOINT ["/entrypoint.sh"]
