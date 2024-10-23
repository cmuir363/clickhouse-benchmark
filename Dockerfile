FROM maven:3.9.9-amazoncorretto-17-debian
WORKDIR /iot-simulator
COPY ./iot-simulator .
RUN  apt-get update -y && apt-get install -y gettext-base python3 python3-pip && python3 --version
RUN pip3 install aiven-client --break-system-packages && avn --version
CMD envsubst < ./src/main/resources/config.template > ./src/main/resources/config.properties && \ 
    echo $AVN_TOKEN | avn user login $AVN_USER --token && \
    avn project switch $AVN_PROJECT && \
    avn service user-kafka-java-creds $AVN_SERVICE_NAME --username avnadmin -d ./certs --password $KAFKA_STORE_PASSWORD && \
    mvn clean package && \
    java -jar target/iot-simulator-1.0-SNAPSHOT.jar