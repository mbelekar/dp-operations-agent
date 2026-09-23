package com.dpopsagent;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reads the demo `orders` topic, applies a deliberately slow map (so there is
 * something real for the backpressure tool to observe), and writes to
 * `orders-sink`. Exists only to give the live Flink diagnostic tools a real
 * job/vertex to query, not a realistic pipeline.
 */
public class OrdersProcessingJob {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv()
                .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5_000L));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("orders")
                .setGroupId("flink-orders-processing")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // forBoundedOutOfOrderness alone defaults to a no-op timestamp
        // assigner (always Long.MIN_VALUE), so the watermark never advances
        // no matter how much data flows. withTimestampAssigner is required
        // to actually use the Kafka record's own timestamp as event time.
        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, recordTimestamp) -> recordTimestamp)
                        .withIdleness(Duration.ofSeconds(30)),
                "orders-source");

        DataStream<String> processed = stream.map(new SlowMap());

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("orders-sink")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        processed.sinkTo(sink);

        env.execute("orders-processing-job");
    }

    private static class SlowMap implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            Thread.sleep(50);
            return value;
        }
    }
}
