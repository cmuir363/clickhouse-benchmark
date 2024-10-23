package example.iot.aiven;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class SensorMetaDataLoader {

    private final List<SensorMetaData> sensors = new ArrayList<>();
    private final Random random = new Random();

    public SensorMetaDataLoader(String filePath) {
        loadSensorsFromCsv(filePath);
    }

    private void loadSensorsFromCsv(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            // Skip the header
            br.readLine();

            // Read each line from the CSV
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length == 4) {
                    String ownerId = fields[0];
                    String factoryId = fields[1];
                    String sensorId = fields[2];
                    String sensorTypeString = fields[3].toUpperCase();
                    SensorType sensorType = SensorType.valueOf(sensorTypeString);

                    // Create a Sensor object and add it to the list
                    SensorMetaData sensorMetadata = new SensorMetaData(ownerId, factoryId, sensorId, sensorType);
                    sensors.add(sensorMetadata);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SensorMetaData getRandomSensor() {
        if (sensors.isEmpty()) {
            return null;
        }
        int randomIndex = random.nextInt(sensors.size());
        return sensors.get(randomIndex);
    }

    public static void main(String[] args) {
        SensorMetaDataLoader dataStore = new SensorMetaDataLoader("src/main/resources/sensors.csv");

        // Get and print a random sensor
        SensorMetaData randomSensor = dataStore.getRandomSensor();
        System.out.println(randomSensor);
    }
}