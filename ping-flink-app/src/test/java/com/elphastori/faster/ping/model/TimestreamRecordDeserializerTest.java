package com.elphastori.faster.ping.model;

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
    public void testPingDeserialize() {
        String jsonString = getDefaultJsonPing().toString();
        Record record = bytesToRecord(jsonString.getBytes(
                StandardCharsets.UTF_8));
        Assertions.assertTrue(record.hasDimensions());
        Assertions.assertTrue(record.hasMeasureValues());
        Assertions.assertEquals(MeasureValueType.MULTI,
                record.measureValueType());
        Assertions.assertEquals("ping_record", record.measureName());
        Assertions.assertEquals("1651516045744", record.time());
        Assertions.assertEquals(TimeUnit.MILLISECONDS, record.timeUnit());

        // check dimensions
        Assertions.assertEquals(1, record.dimensions().size());
        assertDimensionExists(record, "host", "RaspberryPi");

        // check measure values
        Assertions.assertEquals(3, record.measureValues().size());
        assertMeasureValueExists(record, "google", "9.03");
        assertMeasureValueExists(record, "facebook", "59.73");
        assertMeasureValueExists(record, "amazon", "17.06");
    }

    @Test
    public void testDeserializeDoubleNegativeZero() {
        final JSONObject defaultJsonPing = getDefaultJsonPing();
        defaultJsonPing.remove("google");
        defaultJsonPing.put("google", 9.03); //unfortunately even if you put -0.0 here, it gets serialized as -0, which is different from negative zero
        String jsonString = defaultJsonPing.toString();
        jsonString = jsonString.replace("\"google\":9.03", "\"google\":-0.0");
        Record record = bytesToRecord(jsonString.getBytes(StandardCharsets.UTF_8));
        assertMeasureValueExists(record, "google", "0.0");
    }

    private JSONObject getDefaultJsonPing() {
        JSONObject inputJson = new JSONObject();
        inputJson.put("time", "2022-05-02T18:27:25.744Z");
        inputJson.put("google", 9.03);
        inputJson.put("facebook", 59.73);
        inputJson.put("amazon", 17.06);
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
