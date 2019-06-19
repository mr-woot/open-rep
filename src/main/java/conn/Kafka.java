package conn;

import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import sun.security.krb5.Config;
import util.ConfigBundle;

import java.util.Properties;

/**
 * Contributed By: Tushar Mudgal
 * On: 14/6/19 | 4:35 PM
 */
@Log4j
public class Kafka {

    public static KafkaProducer<String, String> producer;

    public static void createConnection(){
        Properties props = new Properties();
        props.put("bootstrap.servers", ConfigBundle.getValue("kafka.servers"));
        props.put("client.id", ConfigBundle.getValue("kafka.clientId"));
        props.put("acks", ConfigBundle.getValue("kafka.acks"));
        props.put("retries", Integer.parseInt(ConfigBundle.getValue("kafka.retries")));
        props.put("batch.size", Integer.parseInt(ConfigBundle.getValue("kafka.batchSize")));
        props.put("linger.ms", Integer.parseInt(ConfigBundle.getValue("kafka.lingerMs")));
        props.put("buffer.memory", Integer.parseInt(ConfigBundle.getValue("kafka.bufferMemory")));

        props.put("kafka.compressionType", ConfigBundle.getValue("kafka.compressionType"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public static void closeConnection(){
        producer.close();
    }
}
