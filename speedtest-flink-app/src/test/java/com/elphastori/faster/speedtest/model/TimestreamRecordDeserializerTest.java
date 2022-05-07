package com.elphastori.faster.speedtest.model;

import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TimestreamRecordDeserializerTest {
    private TimestreamRecordDeserializer deserializer;

    @BeforeAll
    public void init() {
        deserializer = new TimestreamRecordDeserializer();
    }

    @Test
    public void testSpeedtestDeserialize() {
        String jsonString = getDefaultJsonSpeedtest().toString();
        Record record = bytesToRecord(jsonString.getBytes(
                StandardCharsets.UTF_8));
        Assertions.assertTrue(record.hasDimensions());
        Assertions.assertTrue(record.hasMeasureValues());
        Assertions.assertEquals(MeasureValueType.MULTI,
                record.measureValueType());
        Assertions.assertEquals("speedtest_record", record.measureName());
        Assertions.assertEquals("1651516045744", record.time());
        Assertions.assertEquals(TimeUnit.MILLISECONDS, record.timeUnit());

        // check dimensions
        Assertions.assertEquals(1, record.dimensions().size());
        assertDimensionExists(record, "host", "RaspberryPi");

        // check measure values
        Assertions.assertEquals(4, record.measureValues().size());
        assertMeasureValueExists(record, "ping", "9.03");
        assertMeasureValueExists(record, "jitter", "1.77");
        assertMeasureValueExists(record, "download", "64.27");
        assertMeasureValueExists(record, "upload", "17.65");
    }

    @Test
    public void testDeserializeDoubleNegativeZero() {
        final JSONObject defaultJsonSpeedtest = getDefaultJsonSpeedtest();
        defaultJsonSpeedtest.remove("ping");
        defaultJsonSpeedtest.put("ping", 9.03); //unfortunately even if you put -0.0 here, it gets serialized as -0, which is different from negative zero
        String jsonString = defaultJsonSpeedtest.toString();
        jsonString = jsonString.replace("\"ping\":9.03", "\"ping\":-0.0");
        Record record = bytesToRecord(jsonString.getBytes(StandardCharsets.UTF_8));
        assertMeasureValueExists(record, "ping", "0.0");
    }

    private JSONObject getDefaultJsonSpeedtest() {
        JSONObject inputJson = new JSONObject();
        inputJson.put("time", "2022-05-02T18:27:25.744Z");
        inputJson.put("ping", 9.03);
        inputJson.put("jitter", 1.77);
        inputJson.put("download", 64.27);
        inputJson.put("upload", 17.65);
        inputJson.put("host", "RaspberryPi");
        return inputJson;
    }

    private Record bytesToRecord(byte[] bytes) {
        return TimestreamRecordConverter.convert(deserializer.deserialize(bytes));
    }

    // Inefficient, optimize to set if needed
    private void assertMeasureValueExists(Record record, String key, String val) {
        final Optional<MeasureValue> measureValue = record.measureValues()
                .stream().filter(mv -> mv.name().equals(key)).findFirst();
        Assertions.assertTrue(measureValue.isPresent());
        assertValueEquals(measureValue.get().type(), val, measureValue.get().value());
    }

    private void assertDimensionExists(Record record, String key, String val) {
        final Optional<Dimension> dimension = record.dimensions()
                .stream().filter(mv -> mv.name().equals(key)).findFirst();
        Assertions.assertTrue(dimension.isPresent());
        Assertions.assertEquals(val, dimension.get().value());
    }

    private void assertValueEquals(MeasureValueType type, String val1, String val2) {
        if (type.equals(MeasureValueType.DOUBLE)) {
            double double1 = Double.parseDouble(val1);
            double double2 = Double.parseDouble(val2);
            Assertions.assertEquals(double1, double2, 0.0000000001);
        } else {
            Assertions.assertEquals(val1, val2);
        }
    }
}
