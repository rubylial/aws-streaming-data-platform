package com.streaming.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * PE4 — Test local con Flink MiniCluster.
 *
 * Ejecuta el pipeline completo (Event Time + Watermarks + ventanas +
 * agregaciones stateful + checkpoints) usando eventos sintéticos en
 * memoria, SIN conexión a Kinesis.
 *
 * Ejecución:
 *   mvn test -Dtest=StreamingJobLocalTest
 *
 * Evidencia: capturar la salida de consola con los MerchantAgg results.
 */
public class StreamingJobLocalTest {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(4)
                            .setNumberTaskManagers(1)
                            .build());

    /**
     * Genera eventos de prueba distribuidos en 3 minutos,
     * 3 merchants, con montos variados para validar agregaciones.
     */
    private static List<TransactionEvent> generateTestEvents() {
        // Base: 2026-01-15T10:00:00Z
        String base = "2026-01-15T10:0";

        return Arrays.asList(
            // Minuto 0 (10:00:00 - 10:00:59) — merchant-001
            new TransactionEvent("evt-001", base + "0:05Z", "merchant-001", 100.00, "USD", "approved"),
            new TransactionEvent("evt-002", base + "0:15Z", "merchant-001", 250.50, "USD", "approved"),
            new TransactionEvent("evt-003", base + "0:30Z", "merchant-001",  75.00, "USD", "pending"),

            // Minuto 0 — merchant-002
            new TransactionEvent("evt-004", base + "0:10Z", "merchant-002", 500.00, "USD", "approved"),
            new TransactionEvent("evt-005", base + "0:45Z", "merchant-002", 120.00, "USD", "approved"),

            // Minuto 1 (10:01:00 - 10:01:59) — merchant-001
            new TransactionEvent("evt-006", base + "1:05Z", "merchant-001", 300.00, "USD", "approved"),
            new TransactionEvent("evt-007", base + "1:20Z", "merchant-001",  50.00, "USD", "declined"),

            // Minuto 1 — merchant-002
            new TransactionEvent("evt-008", base + "1:15Z", "merchant-002", 800.00, "USD", "approved"),

            // Minuto 1 — merchant-003 (nuevo merchant)
            new TransactionEvent("evt-009", base + "1:30Z", "merchant-003", 1500.00, "USD", "approved"),
            new TransactionEvent("evt-010", base + "1:50Z", "merchant-003",  200.00, "USD", "pending"),

            // Minuto 2 (10:02:00 - 10:02:59) — mezcla
            new TransactionEvent("evt-011", base + "2:00Z", "merchant-001", 175.00, "USD", "approved"),
            new TransactionEvent("evt-012", base + "2:10Z", "merchant-002",  90.00, "USD", "approved"),
            new TransactionEvent("evt-013", base + "2:30Z", "merchant-003", 450.00, "USD", "approved"),

            // Evento tardío (late event): timestamp en minuto 0, llega después
            new TransactionEvent("evt-014", base + "0:55Z", "merchant-001", 60.00, "USD", "approved")
        );
    }

    @Test
    public void testFullPipelineWithMiniCluster() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Checkpoints: misma config que producción ---
        env.enableCheckpointing(5000L); // 5s para test (60s en prod)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.getCheckpointConfig().setCheckpointTimeout(30000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Paralelismo local
        env.setParallelism(2);

        // --- Source: eventos sintéticos ---
        List<TransactionEvent> testEvents = generateTestEvents();

        WatermarkStrategy<TransactionEvent> watermarkStrategy = WatermarkStrategy
                .<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getEventTimeMillis());

        DataStream<TransactionEvent> events = env
                .fromCollection(testEvents)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("test-source");

        // --- Ventana Tumbling 1 minuto ---
        DataStream<MerchantAggregation> tumblingResults = events
                .keyBy(TransactionEvent::getMerchantId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(
                    new StreamingJob.CountSumAggregate(),
                    new StreamingJob.MerchantWindowFunction())
                .name("tumbling-1min-test");

        tumblingResults
                .map(agg -> "[TUMBLING] " + agg.toString())
                .print()
                .name("tumbling-output");

        // --- Ventana Sliding 5 min / 1 min ---
        DataStream<MerchantAggregation> slidingResults = events
                .keyBy(TransactionEvent::getMerchantId)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(
                    new StreamingJob.CountSumAggregate(),
                    new StreamingJob.MerchantWindowFunction())
                .name("sliding-5min-1min-test");

        slidingResults
                .map(agg -> "[SLIDING]  " + agg.toString())
                .print()
                .name("sliding-output");

        // --- Detección de anomalías con KeyedState ---
        DataStream<String> anomalies = events
                .keyBy(TransactionEvent::getMerchantId)
                .flatMap(new TransactionAnomalyDetector())
                .name("anomaly-detection-test");

        anomalies
                .map(alert -> "[ANOMALY]  " + alert)
                .print()
                .name("anomaly-output");

        // --- Ejecutar ---
        System.out.println("=== PE4 Local MiniCluster Test ===");
        System.out.println("Eventos de prueba: " + testEvents.size());
        System.out.println("Merchants: merchant-001, merchant-002, merchant-003");
        System.out.println("Ventanas: 3 minutos (10:00 - 10:02)");
        System.out.println("Late event: evt-014 (timestamp 10:00:55, llega al final)");
        System.out.println("Checkpoints: habilitados cada 5s");
        System.out.println("Paralelismo: 2");
        System.out.println("==================================");

        env.execute("PE4-LocalTest");

        System.out.println("=== Test completado exitosamente ===");
    }
}
