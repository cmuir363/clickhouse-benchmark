package example.iot.aiven;

public class SensorMetaData {
    private final String ownerId;
    private final String factoryId;
    private final String sensorId;
    private final SensorType sensorType;

    public SensorMetaData(String ownerId, String factoryId, String sensorId, SensorType sensorType) {
        this.ownerId = ownerId;
        this.factoryId = factoryId;
        this.sensorId = sensorId;
        this.sensorType = sensorType;
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

    public SensorType getSensorType() {
        return sensorType;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "ownerId='" + ownerId + '\'' +
                ", factoryId='" + factoryId + '\'' +
                ", sensorId='" + sensorId + '\'' +
                ", sensorType='" + sensorType + '\'' +
                '}';
    }
}
