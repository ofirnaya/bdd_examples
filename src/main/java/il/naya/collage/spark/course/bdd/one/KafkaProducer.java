package il.naya.collage.spark.course.bdd.one;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaProducer {

    public static void main(String[] args) throws InterruptedException {

        String topic = "my-topic";
        String bootstrapServer =
                "kafka:9092";
        int chuckMessageCount = 100;
        int numOfChunks = 10;
        String messageTemplate = "{\"message_id\": %d}";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        for (int chunk = 0; chunk < numOfChunks; chunk++){
            for (int messageIdx = 1; messageIdx <= chuckMessageCount; messageIdx++){
                int messageNumber = (chuckMessageCount * chunk) + messageIdx;

                String messageContent = String.format(messageTemplate, messageNumber);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, messageContent);

                producer.send(record);
            }
            producer.flush();
            Thread.sleep(2000);
        }

        producer.close();

    }
}
