package com.streaming.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests unitarios para TransactionEvent y deserialización.
 * No requiere AWS ni Kinesis — todo es local.
 */
public class TransactionEventTest {

    private static final String SAMPLE_JSON = "{"
            + "\"event_id\":\"evt-test-001\","
            + "\"event_time\":\"2024-11-04T05:00:00Z\","
            + "\"merchant_id\":\"merchant-001\","
            + "\"amount\":125000.50,"
            + "\"currency\":\"COP\","
            + "\"status\":\"completed\""
            + "}";

    @Test
    public void testDeserializeFromJson() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TransactionEvent event = mapper.readValue(SAMPLE_JSON, TransactionEvent.class);

        assertEquals("evt-test-001", event.getEventId());
        assertEquals("2024-11-04T05:00:00Z", event.getEventTime());
        assertEquals("merchant-001", event.getMerchantId());
        assertEquals(125000.50, event.getAmount(), 0.01);
        assertEquals("COP", event.getCurrency());
        assertEquals("completed", event.getStatus());
    }

    @Test
    public void testEventTimeMillis() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TransactionEvent event = mapper.readValue(SAMPLE_JSON, TransactionEvent.class);

        long millis = event.getEventTimeMillis();
        assertTrue("epoch millis debe ser positivo", millis > 0);
        assertEquals(1730696400000L, millis);
    }

    @Test
    public void testDeserializerHandlesNullFields() throws Exception {
        String partial = "{\"event_id\":\"evt-002\",\"event_time\":\"2024-11-04T05:00:00Z\"}";
        ObjectMapper mapper = new ObjectMapper();
        TransactionEvent event = mapper.readValue(partial, TransactionEvent.class);

        assertEquals("evt-002", event.getEventId());
        assertNull(event.getMerchantId());
        assertEquals(0.0, event.getAmount(), 0.01);
    }

    @Test
    public void testDeserializerIgnoresUnknownFields() throws Exception {
        String extra = SAMPLE_JSON.replace("}", ",\"extra_field\":\"ignored\"}");
        ObjectMapper mapper = new ObjectMapper();
        TransactionEvent event = mapper.readValue(extra, TransactionEvent.class);

        assertEquals("evt-test-001", event.getEventId());
    }

    @Test
    public void testTransactionDeserializerMalformed() throws Exception {
        TransactionDeserializer deser = new TransactionDeserializer();
        TransactionEvent result = deser.deserialize("not-json".getBytes());
        assertNull("Evento malformado debe retornar null", result);
    }

    @Test
    public void testTransactionDeserializerValid() throws Exception {
        TransactionDeserializer deser = new TransactionDeserializer();
        TransactionEvent result = deser.deserialize(SAMPLE_JSON.getBytes());

        assertNotNull(result);
        assertEquals("evt-test-001", result.getEventId());
        assertEquals("merchant-001", result.getMerchantId());
    }

    @Test
    public void testMerchantAggregationToString() {
        MerchantAggregation agg = new MerchantAggregation(
                "merchant-001",
                "2024-11-04T05:00:00Z",
                "2024-11-04T05:01:00Z",
                10, 500000.0, 50000.0
        );
        String str = agg.toString();
        assertTrue(str.contains("merchant-001"));
        assertTrue(str.contains("count=10"));
    }
}
