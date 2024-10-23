package example.iot.aiven;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

public class Main {
    public static void main(String[] args)  {

    SensorMetaDataLoader dataStore = new SensorMetaDataLoader("src/main/resources/sensors.csv");
    Configurations configs = new Configurations();
    int messageFrequency=1;
    
    try {
        Configuration config = configs.properties("config.properties");
        messageFrequency = config.getInt("message.frequency");
    } catch(ConfigurationException e) {
        e.printStackTrace();
    }

    Producer producer = new Producer("iot_measurements");

    while(true) {
        // Get random sensor
        SensorMetaData randomSensorMetaData = dataStore.getRandomSensor();
        // Generate a value for this sensor
        SensorSimulator sensorSimulator = new SensorSimulator();
        CompleteSensorData randomSensorData =
                sensorSimulator.generateData(randomSensorMetaData.getOwnerId(), randomSensorMetaData.getFactoryId(),
                        randomSensorMetaData.getSensorId(), randomSensorMetaData.getSensorType());

        producer.produceIotMessage(randomSensorData);
        System.out.println(randomSensorData.toString());
        try {
            Thread.sleep(1000/messageFrequency);
        } catch(InterruptedException ex){}
        }
    }
}