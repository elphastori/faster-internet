package com.elphastori.faster.ping.model;

import java.time.Instant;
import java.util.List;

import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;

public class TimestreamRecordConverter {
    public static Record convert(final Ping ping) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("host")
                        .value(ping.getHost()).build()
        );

        List<MeasureValue> measureValues = List.of(
                MeasureValue.builder()
                        .name("google")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(ping.getGoogle())).build(),
                MeasureValue.builder()
                        .name("facebook")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(ping.getFacebook())).build(),
                MeasureValue.builder()
                        .name("amazon")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(ping.getAmazon())).build()
        );

        long time = Instant.parse(ping.getTime()).toEpochMilli();

        return Record.builder()
                .dimensions(dimensions)
                .measureName("ping_record")
                .measureValueType("MULTI")
                .measureValues(measureValues)
                .timeUnit(TimeUnit.MILLISECONDS)
                .time(Long.toString(time)).build();
    }

    private static String doubleToString(double inputDouble) {
        // Avoid sending -0.0 (negative double) to Timestream - it throws ValidationException
        if (Double.valueOf(-0.0).equals(inputDouble)) {
            return "0.0";
        }
        return Double.toString(inputDouble);
    }
}
