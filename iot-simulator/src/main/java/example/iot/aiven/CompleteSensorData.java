package example.iot.aiven;

import java.time.Instant;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class CompleteSensorData {
    private final String ownerId;
    private final String factoryId;
    private final String sensorId;
    private final Instant timestamp;
    private final SensorType sensorType;
    private final double value;

    public CompleteSensorData(String ownerId, String factoryId, String sensorId, SensorType sensorType, double value) {
        this.ownerId = ownerId;
        this.factoryId = factoryId;
        this.sensorId = sensorId;
        this.timestamp = Instant.now();
        this.sensorType = sensorType;
        this.value = value;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public String getFactoryId() {
        return factoryId;
    }

    public String getSensorId() {
        return sensorId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public SensorType getSensorType() {
        return sensorType;
    }

    public double getValue() {
        return value;
    }


    public String toJson() {
        String jsonString = "";
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
            mapper.configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
            jsonString = mapper.writeValueAsString(this);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "ownerId='" + ownerId + '\'' +
                ", factoryId='" + factoryId + '\'' +
                ", sensorId='" + sensorId + '\'' +
                ", timestamp=" + timestamp +
                ", sensorType=" + sensorType +
                ", value=" + value +
                '}';
    }
}
