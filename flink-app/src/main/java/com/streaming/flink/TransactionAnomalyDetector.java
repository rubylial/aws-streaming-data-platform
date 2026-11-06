package com.streaming.flink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PE4 — KeyedState explícito con ValueState.
 *
 * Detecta transacciones anómalas por merchant usando un promedio
 * móvil almacenado en ValueState. Una transacción se marca como
 * anómala si su monto supera 2x el promedio histórico del merchant.
 *
 * State gestionado:
 *   - runningSum  (ValueState<Double>): suma acumulada de montos
 *   - runningCount (ValueState<Long>):  conteo acumulado de transacciones
 *
 * Ambos son parte del state de Flink, se incluyen en checkpoints
 * y se recuperan automáticamente ante fallos.
 *
 * Debe aplicarse después de keyBy(merchantId).
 */
public class TransactionAnomalyDetector
        extends RichFlatMapFunction<TransactionEvent, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionAnomalyDetector.class);

    /** Umbral: transacción anómala si monto > THRESHOLD * promedio. */
    private static final double THRESHOLD = 2.0;

    /** Mínimo de transacciones antes de evaluar anomalías. */
    private static final long MIN_EVENTS_FOR_DETECTION = 3;

    private transient ValueState<Double> runningSum;
    private transient ValueState<Long> runningCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        // ValueState para la suma acumulada
        ValueStateDescriptor<Double> sumDescriptor =
                new ValueStateDescriptor<>("running-sum", Types.DOUBLE);
        runningSum = getRuntimeContext().getState(sumDescriptor);

        // ValueState para el conteo acumulado
        ValueStateDescriptor<Long> countDescriptor =
                new ValueStateDescriptor<>("running-count", Types.LONG);
        runningCount = getRuntimeContext().getState(countDescriptor);
    }

    @Override
    public void flatMap(TransactionEvent event, Collector<String> out) throws Exception {
        // Leer estado actual (null si es el primer evento para este key)
        Double currentSum = runningSum.value();
        Long currentCount = runningCount.value();

        if (currentSum == null) currentSum = 0.0;
        if (currentCount == null) currentCount = 0L;

        // Evaluar anomalía solo si hay suficiente historial
        if (currentCount >= MIN_EVENTS_FOR_DETECTION) {
            double avgAmount = currentSum / currentCount;
            if (event.getAmount() > THRESHOLD * avgAmount) {
                String alert = String.format(
                    "ANOMALY DETECTED | merchant=%s | event_id=%s | amount=%.2f | "
                    + "avg=%.2f | threshold=%.2f | ratio=%.2fx",
                    event.getMerchantId(),
                    event.getEventId(),
                    event.getAmount(),
                    avgAmount,
                    THRESHOLD * avgAmount,
                    event.getAmount() / avgAmount
                );
                LOG.warn(alert);
                out.collect(alert);
            }
        }

        // Actualizar estado con el nuevo evento
        runningSum.update(currentSum + event.getAmount());
        runningCount.update(currentCount + 1);

        LOG.debug("State updated | merchant={} | count={} | sum={:.2f}",
                event.getMerchantId(), currentCount + 1, currentSum + event.getAmount());
    }
}
