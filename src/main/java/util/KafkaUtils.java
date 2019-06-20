package util;

import conn.Kafka;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Contributed By: Tushar Mudgal
 * On: 14/6/19 | 4:34 PM
 */
@Log4j
public class KafkaUtils {
    public static void sendMessage(String topic, String message) {
//        Kafka.producer.send(new ProducerRecord<String, String>(topic, message));
        log.info("Message produced: " + message);
    }
}
