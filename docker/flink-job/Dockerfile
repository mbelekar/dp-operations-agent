FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM flink:1.18.1-scala_2.12-java11
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/orders-processing-job-1.0.0.jar /opt/job/orders-processing-job.jar
COPY submit.sh /submit.sh
RUN chmod +x /submit.sh
ENTRYPOINT ["/submit.sh"]
