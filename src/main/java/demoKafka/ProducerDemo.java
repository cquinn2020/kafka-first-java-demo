package demoKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Hello World!");

        KafkaTopicCreator topicCreator = new KafkaTopicCreator("127.0.0.1:9092");
        topicCreator.createTopic("demo_java");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo_java", "hello world");
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("demo_java", "hello world 2");

        // send data - asynchronous
        producer.send(record);
        producer.send(record2);

        // tell the producer to send data and block until done -- sync
        producer.flush();

        // flush and close producer
        producer.close();

        // describe topic
        topicCreator.describeTopic("demo_java");


    }
}
