package il.naya.collage.spark.course.bdd.one;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args){

        String topic = "topic1";
        String bootstrapServers = "kafka:9092";
        String groupId = "my-group";

        int giveUp = 100;
        int noRecordsCount = 0;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Set the maximum of messages return by poll command
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topic));

        while (true){
            ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(1000);

            consumerRecords.forEach(currentRecord -> {
                System.out.println(currentRecord.value());
            });
        }

    }
}
