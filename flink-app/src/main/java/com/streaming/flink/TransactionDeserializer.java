package com.streaming.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializador JSON -> TransactionEvent para el conector Kinesis.
 *
 * Eventos malformados se loguean y se descartan (no rompen el job).
 */
public class TransactionDeserializer extends AbstractDeserializationSchema<TransactionEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);

    private transient ObjectMapper mapper;

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
        }
        return mapper;
    }

    @Override
    public TransactionEvent deserialize(byte[] message) throws IOException {
        try {
            return getMapper().readValue(message, TransactionEvent.class);
        } catch (Exception e) {
            LOG.warn("Error deserializando evento, descartando: {}", e.getMessage());
            return null;
        }
    }
}
