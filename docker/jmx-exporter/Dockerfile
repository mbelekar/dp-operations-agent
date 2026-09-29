# Scrapes a single Kafka broker's JMX and re-exposes only the two metrics
# LiveKafkaGateway.broker_jmx_metrics needs (IsrShrinksPerSec/IsrExpandsPerSec),
# with a static broker= label set per instance via BROKER_ID.
FROM eclipse-temurin:11-jre-jammy@sha256:68607af464c53b34d7dd3ee25697d53531ec46a80326be95129346ff74ae6f8f

ARG JMX_EXPORTER_VERSION=1.0.1
# SHA-256 of that jar. Maven Central publishes only a SHA-1 and a PGP
# signature for it; the jar was checked against both (the signing key is the
# one whose fingerprint prometheus/jmx_exporter publishes in SIGNING_KEY.txt)
# before this hash was taken. Bump both ARGs together.
ARG JMX_EXPORTER_SHA256=a493eb637f9eb827dc3bee2a8cb0dc6bf2b291ace896d4ebfe98b055e394ab1b

RUN apt-get update \
    && apt-get install -y --no-install-recommends gettext-base curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /opt/jmx_prometheus_httpserver.jar \
       "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_httpserver/${JMX_EXPORTER_VERSION}/jmx_prometheus_httpserver-${JMX_EXPORTER_VERSION}.jar" \
    && echo "${JMX_EXPORTER_SHA256}  /opt/jmx_prometheus_httpserver.jar" | sha256sum -c -

COPY kafka-broker.yml.template /etc/jmx-exporter/config.yml.template
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5556
ENTRYPOINT ["/entrypoint.sh"]
