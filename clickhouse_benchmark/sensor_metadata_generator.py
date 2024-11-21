import csv
import random
import uuid

# Configuration
NUM_OWNERS = 25
NUM_FACTORIES = 75
SENSORS_PER_FACTORY = 1500  # Number of sensors per factory
OUTPUT_FILE = "sensors.csv"

# Define the available sensor types
SENSOR_TYPES = [
    "Temperature",
    "Humidity",
    "Pressure",
    "Vibration",
    "Current",
    "Rotation",
]


def generate_sensors(
    num_owners: int, num_factories: int, sensors_per_factory: int
) -> list[list[str]]:
    sensors = []
    for _ in range(num_owners):
        owner_id = f"owner_{uuid.uuid4()}"

        for _ in range(num_factories):
            factory_id = f"factory_{uuid.uuid4()}"

            for _ in range(sensors_per_factory):
                sensor_id = f"sensor_{uuid.uuid4()}"
                sensor_type = random.choice(SENSOR_TYPES)
                sensors.append([owner_id, factory_id, sensor_id, sensor_type])

    return sensors


def write_sensors_to_csv(sensors: list[list[str]], output_file: str) -> None:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["ownerId", "factoryId", "sensorId", "sensorType"])
        writer.writerows(sensors)
    print(f"Sensors generated and saved to {output_file}")


def main() -> None:
    sensors = generate_sensors(NUM_OWNERS, NUM_FACTORIES, SENSORS_PER_FACTORY)
    write_sensors_to_csv(sensors, OUTPUT_FILE)


if __name__ == "__main__":
    main()
