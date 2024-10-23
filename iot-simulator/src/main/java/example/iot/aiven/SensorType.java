package example.iot.aiven;

public enum SensorType {
    TEMPERATURE(1),
    HUMIDITY(2),
    PRESSURE(3),
    VIBRATION(4),
    CURRENT(5),
    ROTATION(6);

    private final int code;

    SensorType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
