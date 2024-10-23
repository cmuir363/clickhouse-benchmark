package example.iot.aiven;

import java.util.Random;

public class SensorSimulator {
    private Random random = new Random();

    public CompleteSensorData generateData(String ownerId, String factoryId, String sensorId, SensorType sensorType) {
        double value = generateSensorValue(sensorType);
        return new CompleteSensorData(ownerId, factoryId, sensorId, sensorType, value);
    }

    private double generateSensorValue(SensorType sensorType) {
        switch (sensorType) {
            case TEMPERATURE:
                return 15.0 + this.random.nextDouble() * 10.0;
            case HUMIDITY:
                return 30.0 + this.random.nextDouble() * 40.0;
            case PRESSURE:
                return 990.0 + this.random.nextDouble() * 20.0;
            case VIBRATION:
                return this.random.nextDouble() * 5.0;
            case CURRENT:
                return 0.0 + this.random.nextDouble() * 50.0;
            case ROTATION:
                return this.random.nextDouble() * 100.0;
            default:
                throw new IllegalArgumentException("Unknown sensor type");
        }
    }


}
