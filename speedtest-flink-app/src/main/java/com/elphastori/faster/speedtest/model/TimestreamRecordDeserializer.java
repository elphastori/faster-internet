package com.elphastori.faster.speedtest.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TimestreamRecordDeserializer implements DeserializationSchema<Speedtest> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Speedtest deserialize(byte[] messageBytes) {
        try {
            return objectMapper.readValue(messageBytes, Speedtest.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }

    @Override
    public boolean isEndOfStream(Speedtest nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Speedtest> getProducedType() {
        return TypeInformation.of(Speedtest.class);
    }
}
