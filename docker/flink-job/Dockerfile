FROM maven:3.9-eclipse-temurin-11@sha256:7bb84a451705605a47d24eb844d4d9ab03e33e131a507db76aafe25db3907f64 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM flink:1.18.1-scala_2.12-java11@sha256:f0e9ffd07f3d43291dfaf7e0609eb2871462193275d452affbe5fac091017ae3
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/orders-processing-job-1.0.0.jar /opt/job/orders-processing-job.jar
COPY submit.sh /submit.sh
RUN chmod +x /submit.sh
ENTRYPOINT ["/submit.sh"]
