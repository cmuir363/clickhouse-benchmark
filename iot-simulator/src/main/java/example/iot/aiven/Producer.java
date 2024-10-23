package example.iot.aiven;

import java.util.Properties;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

public final class Producer {
    private KafkaProducer<String, String> producer;
    private String currentTopic;
    private String bootstrapServers;
    private String truststorePassword = "";
    private String keystorePassword = "";
    private String keyPassword = "";
    private String certsLocation = "";
    private String securityProtocol = "";
    private String sslTrustStoreFilename = "";
    private String sslKeystoreFilename = "";
    public Producer (String topic) {
        this.currentTopic = topic;
        Configurations configs = new Configurations();
        try {
            Configuration config = configs.properties("config.properties");

            this.bootstrapServers = config.getString("bootstrap.servers");
            this.keystorePassword = config.getString("keystore.password");
            this.truststorePassword = config.getString("truststore.password");
            this.keyPassword = config.getString("key.password");
            this.certsLocation = config.getString("certs.location");
            this.securityProtocol = config.getString("security.protocol");
            this.sslTrustStoreFilename = config.getString("ssl.truststore.filename");
            this.sslKeystoreFilename = config.getString("ssl.keystore.filename");

        } catch (ConfigurationException cex) {
            cex.printStackTrace();
        }

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", this.bootstrapServers );
        properties.setProperty("security.protocol", this.securityProtocol);
        properties.setProperty("ssl.truststore.location", this.certsLocation + this.sslTrustStoreFilename);
        properties.setProperty("ssl.truststore.password", this.truststorePassword);
        properties.setProperty("ssl.keystore.type", "PKCS12");
        properties.setProperty("ssl.keystore.location", this.certsLocation + this.sslKeystoreFilename);
        properties.setProperty("ssl.keystore.password", this.keystorePassword);
        properties.setProperty("ssl.key.password", this.keyPassword);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create a producer
        this.producer = new KafkaProducer<>(properties);
    }

    public void produceIotMessage(CompleteSensorData sensorData) {
        producer.send(new ProducerRecord<String, String>(this.currentTopic, sensorData.toJson()));
    }

    public void closeProducer() {
        producer.close();
    }
}
