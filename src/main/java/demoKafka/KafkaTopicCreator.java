package demoKafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTopicCreator {
    private AdminClient adminClient;

    public KafkaTopicCreator(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        this.adminClient = AdminClient.create(properties);
    }

    public void createTopic(String topicName) {
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1); // topic name, number of partitions, replication factor
        this.adminClient.createTopics(Collections.singletonList(newTopic));
    }

    public void describeTopic(String topicName) {
        DescribeTopicsResult describeTopicsResult = this.adminClient.describeTopics(Collections.singletonList(topicName));
        try {
            Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
            TopicDescription topicDescription = topicDescriptionMap.get(topicName);
            System.out.println("Topic: " + topicDescription.name());
            System.out.println("Partitions: " + topicDescription.partitions());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
