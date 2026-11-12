package com.streaming.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * PE4/PE5 — Flink Stateful Streaming Job + Iceberg Sink.
 *
 * Consume eventos JSON desde Kinesis Data Streams.
 * Aplica Event Time + Watermarks con tolerancia de 5 segundos.
 * Ventana Tumbling de 1 minuto: count, sum, avg por merchant.
 * Ventana Sliding de 5 min / slide 1 min: tendencia por merchant.
 * Checkpoints habilitados cada 60 segundos.
 *
 * PE5: sink configurable a Iceberg vía Glue Data Catalog.
 *
 * Parámetros:
 *   --stream.name          Nombre del Kinesis stream
 *   --aws.region           Región AWS (default: us-east-1)
 *   --checkpoint.interval  Intervalo de checkpoint ms (default: 60000)
 *   --sink.mode            print | iceberg (default: print)
 *   --iceberg.warehouse    Ruta S3 del warehouse Iceberg
 *   --iceberg.database     Nombre de la Glue database
 *   --iceberg.catalog      Nombre del catálogo (default: glue_catalog)
 */
public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String streamName = params.get("stream.name", "streaming-data-platform-events-dev");
        String region = params.get("aws.region", "us-east-1");
        long checkpointInterval = params.getLong("checkpoint.interval", 60000L);
        String sinkMode = params.get("sink.mode", "print");

        // Parámetros Iceberg (PE5)
        String icebergWarehouse = params.get("iceberg.warehouse",
                "s3://streaming-data-platform-lakehouse-dev/warehouse");
        String icebergDatabase = params.get("iceberg.database",
                "streaming_data_platform_dev");
        String icebergCatalog = params.get("iceberg.catalog", "glue_catalog");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Checkpoints: cada 60s, exactly-once ---
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
        env.getCheckpointConfig().setCheckpointTimeout(120000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        LOG.info("=== PE4/PE5 Flink Streaming Job ===");
        LOG.info("Stream: {}, Region: {}, Checkpoint: {}ms", streamName, region, checkpointInterval);
        LOG.info("Sink mode: {}", sinkMode);

        // --- Fuente Kinesis ---
        Properties kinesisProps = new Properties();
        kinesisProps.setProperty("aws.region", region);
        kinesisProps.setProperty("flink.stream.initpos", "LATEST");

        KinesisStreamsSource<TransactionEvent> source = KinesisStreamsSource
                .<TransactionEvent>builder()
                .setStreamArn("arn:aws:kinesis:" + region + ":000000000000:stream/" + streamName)
                .setDeserializationSchema(new TransactionDeserializer())
                .build();

        // --- Watermark Strategy: Event Time con 5s de tolerancia ---
        WatermarkStrategy<TransactionEvent> watermarkStrategy = WatermarkStrategy
                .<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<TransactionEvent>)
                                (event, recordTimestamp) -> event.getEventTimeMillis()
                )
                .withIdleness(Duration.ofSeconds(30));

        DataStream<TransactionEvent> events = env
                .fromSource(source, watermarkStrategy, "Kinesis Source")
                .filter(event -> event != null)
                .name("filter-null-events");

        // --- Ventana Tumbling 1 minuto por merchant ---
        DataStream<MerchantAggregation> tumblingResults = events
                .keyBy(TransactionEvent::getMerchantId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new CountSumAggregate(), new MerchantWindowFunction())
                .name("tumbling-1min-aggregation");

        // --- Ventana Sliding 5 min / slide 1 min por merchant ---
        DataStream<MerchantAggregation> slidingResults = events
                .keyBy(TransactionEvent::getMerchantId)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new CountSumAggregate(), new MerchantWindowFunction())
                .name("sliding-5min-1min-aggregation");

        // --- Detección de anomalías con KeyedState (ValueState) ---
        DataStream<String> anomalies = events
                .keyBy(TransactionEvent::getMerchantId)
                .flatMap(new TransactionAnomalyDetector())
                .name("anomaly-detection-keyed-state");

        // --- Sink: print (local) o Iceberg (AWS) ---
        if ("iceberg".equalsIgnoreCase(sinkMode)) {
            LOG.info("Configurando sink Iceberg: warehouse={}, database={}, catalog={}",
                    icebergWarehouse, icebergDatabase, icebergCatalog);

            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            IcebergSinkBuilder icebergSink = new IcebergSinkBuilder(
                    tableEnv, icebergDatabase, icebergWarehouse, region);

            // Registrar catálogo Glue
            icebergSink.registerCatalog(icebergCatalog);

            // Escribir eventos individuales a transactions
            icebergSink.writeTransactions(events, icebergCatalog);

            // Escribir agregaciones tumbling a merchant_aggregations
            icebergSink.writeMerchantAggregations(tumblingResults, icebergCatalog);

            LOG.info("Sinks Iceberg configurados — transactions + merchant_aggregations");
        } else {
            // Modo local/print: salida por consola (PE4)
            tumblingResults.print().name("tumbling-output");
            slidingResults.print().name("sliding-output");
            anomalies.print().name("anomaly-output");
        }

        env.execute("PE4/PE5-TransactionStreaming");
    }

    // =========================================================
    // Aggregate Function: acumula count y sum
    // =========================================================

    /**
     * Acumulador intermedio para la agregación incremental.
     */
    public static class CountSumAccumulator {
        public long count = 0;
        public double sum = 0.0;
    }

    /**
     * Función de agregación incremental: count + sum por ventana.
     * Se ejecuta en cada evento sin almacenar todos los registros.
     */
    public static class CountSumAggregate
            implements AggregateFunction<TransactionEvent, CountSumAccumulator, CountSumAccumulator> {

        @Override
        public CountSumAccumulator createAccumulator() {
            return new CountSumAccumulator();
        }

        @Override
        public CountSumAccumulator add(TransactionEvent event, CountSumAccumulator acc) {
            acc.count++;
            acc.sum += event.getAmount();
            return acc;
        }

        @Override
        public CountSumAccumulator getResult(CountSumAccumulator acc) {
            return acc;
        }

        @Override
        public CountSumAccumulator merge(CountSumAccumulator a, CountSumAccumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            return a;
        }
    }

    // =========================================================
    // Process Window Function: agrega metadata de ventana
    // =========================================================

    /**
     * Combina el resultado del aggregate con la información de la ventana
     * (start, end) y el key (merchantId).
     */
    public static class MerchantWindowFunction
            extends ProcessWindowFunction<CountSumAccumulator, MerchantAggregation, String, TimeWindow> {

        @Override
        public void process(String merchantId,
                            ProcessWindowFunction<CountSumAccumulator, MerchantAggregation, String, TimeWindow>.Context context,
                            Iterable<CountSumAccumulator> elements,
                            Collector<MerchantAggregation> out) {

            CountSumAccumulator acc = elements.iterator().next();
            TimeWindow window = context.window();

            String windowStart = FORMATTER.format(Instant.ofEpochMilli(window.getStart()));
            String windowEnd = FORMATTER.format(Instant.ofEpochMilli(window.getEnd()));
            double avgAmount = acc.count > 0 ? acc.sum / acc.count : 0.0;

            out.collect(new MerchantAggregation(
                    merchantId, windowStart, windowEnd,
                    acc.count, acc.sum, avgAmount
            ));
        }

        private static final DateTimeFormatter FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    }
}
