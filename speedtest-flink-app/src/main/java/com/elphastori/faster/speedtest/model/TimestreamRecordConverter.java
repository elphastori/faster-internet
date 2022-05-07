package com.elphastori.faster.speedtest.model;

import java.time.Instant;
import java.util.List;

import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;

public class TimestreamRecordConverter {
    public static Record convert(final Speedtest speedtest) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("host")
                        .value(speedtest.getHost()).build()
        );

        List<MeasureValue> measureValues = List.of(
                MeasureValue.builder()
                        .name("ping")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(speedtest.getPing())).build(),
                MeasureValue.builder()
                        .name("jitter")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(speedtest.getJitter())).build(),
                MeasureValue.builder()
                        .name("download")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(speedtest.getDownload())).build(),
                MeasureValue.builder()
                        .name("upload")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(speedtest.getUpload())).build()
        );

        long time = Instant.parse(speedtest.getTime()).toEpochMilli();

        return Record.builder()
                .dimensions(dimensions)
                .measureName("speedtest_record")
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
