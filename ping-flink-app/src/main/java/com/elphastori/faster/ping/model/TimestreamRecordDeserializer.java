package com.elphastori.faster.ping.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TimestreamRecordDeserializer implements DeserializationSchema<Ping> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Ping deserialize(byte[] messageBytes) {
        try {
            return objectMapper.readValue(messageBytes, Ping.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }

    @Override
    public boolean isEndOfStream(Ping nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Ping> getProducedType() {
        return TypeInformation.of(Ping.class);
    }
}
